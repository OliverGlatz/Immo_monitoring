#### Installieren der Packete ####
#install.packages("tidyverse")
#install.packages("rvest")
#install.packages("httr")
#install.packages("doParallel")
#install.packages("doSNOW")
#install.packages("R.utils")

#### Laden der Packete ####
library(tidyverse)
library(rvest)
library(httr)
library(doParallel)
library(doSNOW)
library(R.utils)

#### set options ####
# https://stackoverflow.com/questions/62730783/error-in-makepsockclusternames-spec-cluster-setup-failed-3-of-3-work
if (Sys.getenv("RSTUDIO") == "1" && !nzchar(Sys.getenv("RSTUDIO_TERM")) && 
    Sys.info()["sysname"] == "Darwin" && getRversion() >= "4.0.0") {
  parallel:::setDefaultClusterOptions(setup_strategy = "sequential")
}

#### set up cluster ####
cl <- makeSOCKcluster(detectCores())
registerDoSNOW(cl)

# set options - log fertige tasks
progress <- function(n) cat(sprintf("task %d is complete\n", n))
opts <- list(progress=progress)

# initial each worker
clusterEvalQ(cl, {
  ### load library
  
  library(tidyverse)
  library(rvest)
  library(httr)
  library(R.utils)
  
  ### set variables
  timer_proxy <- Sys.time() - 1001
  
  ### load functions
  
  # Funktion get html 
  fn_get_page_content <- function(pushed_url, max_attempts) {
    # empty page and attempts
    page <- NULL
    attempt = 0
    # loop - mehrmals versuchen mit unterschiedlichen Proxy Seite aufzurufen
    for (attempt in 1:max_attempts) {
      if(is.null(page)){
        index_proxy <- round(runif(1, 1, length(proxy_list)),0)
        full_proxy <- paste0("http://",proxy_list[index_proxy])
        try(page <- read_html(httr::GET(url = pushed_url, httr::set_config(httr::use_proxy(full_proxy)))), silent=TRUE)
      }
    }
    return(page)
  } 
  
  # Anzahl der Ergebnisseiten ermitteln
  fn_scrape_immo_anzahl_seiten <- function(page) {
    # Maximale Anzahl an Ergebnis-Seiten
    max_page <- page %>% 
      html_node(css = "#listings > div > ul > li:nth-child(7) > a") %>%
      html_text() %>%
      str_trim() %>%
      ifelse(identical(., character(0)),NA, .)
    max_page
  }
  
  # Alle Ergebnisseiten nach Inserate-ID, -Link und -Preis der Stadt durchsuchen.
  fn_ergebnisseiten_durchsuchen <- function(page) {
    # IDs von den Inseraten
    Inserate_ID <- page %>% 
      html_nodes("article") %>% 
      html_attr("data-obid")
    if(!identical(Inserate_ID, character(0))){
      # Preis je Inserat über ID
      vec_preis <- c()
      for(i in 1:length(Inserate_ID)){
        preis_inserat <- page %>% 
          html_nodes(paste0("[data-obid='",Inserate_ID[i],"']")) %>%
          html_nodes("[class='font-highlight font-tabular']") %>%
          html_text() %>%
          str_trim() %>%
          ifelse(identical(., character(0)),NA, .)
        vec_preis <- c(vec_preis, preis_inserat)
      }
      return(data.frame(Inserate_ID, vec_preis))
    }
  }
  
  # Funktion get proxies
  fn_get_proxies <- function() {
    
    proxy_list_1 <- read.csv("https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt", header = FALSE)
    proxy_list_1 <- as.vector(proxy_list_1$V1)
    
    # Proxy-Liste von free-proxy-list.net
    proxy_list_2 <- read_html("https://free-proxy-list.net/") %>% 
      html_nodes("[class='form-control']") %>%
      html_text() %>%
      substr(., 76, nchar(.)-1) %>%
      str_replace_all("\\s+" , " ") %>%
      str_split(., " ") %>%
      unlist(.) 
    
    proxy_list <- c(proxy_list_1, proxy_list_2)
    print(paste0("Es stehen ",length(proxy_list), " Proxies zur Verfügung."))
    
    # give proxy_list back
    proxy_list
    
  }
  
  # Letzten Character aus String bekommen
  substrRight <- function(x, n){
    substr(x, nchar(x)-n+1, nchar(x))
  }
  
  # Funktion auslesen ImmobilienScout24 Anzeige ######################################
  fn_scrape_immo_inserat <- function(immo_url, page) {
    
    # id_inserat
    id_inserat <- gsub('https://www.immobilienscout24.de/expose/', '', immo_url) %>% ifelse(identical(., character(0)),NA, .)
    
    # Kaufpreis
    kaufpreis <- page %>% 
      html_nodes("[class='is24qa-kaufpreis grid-item three-fifths']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Kaltmiete
    kaltmiete <- page %>% 
      html_nodes("[class='is24qa-kaltmiete-main is24-value font-semibold is24-preis-value']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Warmmiete
    warmmiete <- page %>% 
      html_nodes("[class='is24qa-warmmiete-main is24-value font-semibold']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Hausgeld   
    hausgeld <- page %>% 
      html_nodes("[class='is24qa-hausgeld grid-item three-fifths']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Provision 
    provision <- page %>% 
      html_nodes("[class='is24qa-provision grid-item two-fifths']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Typ 
    typ <- page %>% 
      html_nodes("[class='is24qa-typ grid-item three-fifths']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Etage 
    etage <- page %>% 
      html_nodes("[class='is24qa-etage grid-item three-fifths']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Wohnflaeche  
    wohnflaeche <- page %>% 
      html_nodes("[class='is24qa-wohnflaeche-ca grid-item three-fifths']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Nutzflaeche
    nutzflaeche <- page %>% 
      html_nodes("[class='is24qa-nutzflaeche-ca grid-item three-fifths']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Grundstück 
    grundstück <- page %>% 
      html_nodes("[class='is24qa-grundstueck-ca grid-item three-fifths']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Bezugsfrei_ab
    bezugsfrei_ab <- page %>% 
      html_nodes("[class='is24qa-bezugsfrei-ab grid-item three-fifths']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Zimmer
    zimmer <- page %>% 
      html_nodes("[class='is24qa-zimmer grid-item three-fifths']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Schlafzimmer
    schlafzimmer <- page %>% 
      html_nodes("[class='is24qa-schlafzimmer grid-item three-fifths']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Badezimmer
    badezimmer <- page %>% 
      html_nodes("[class='is24qa-badezimmer grid-item three-fifths']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Garage_Stellplatz
    garage_stellplatz <- page %>% 
      html_nodes("[class='is24qa-garage-stellplatz grid-item three-fifths']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Adresse
    adresse <- page %>% 
      html_nodes("[class='zip-region-and-country']") %>%
      html_text() %>%
      str_trim() %>% 
      first() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Baujahr
    baujahr <- page %>% 
      html_nodes("[class='is24qa-baujahr grid-item three-fifths']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Energietraeger
    energietraeger <- page %>% 
      html_nodes("[class='is24qa-wesentliche-energietraeger grid-item three-fifths']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Objektbeschreibung
    objektbeschreibung <- page %>% 
      html_nodes("[class='is24qa-objektbeschreibung text-content short-text']") %>%
      html_text() %>%
      str_trim() %>%
      str_replace_all(., "[^[:alnum:]]", " ") %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Ausstattung
    ausstattung <- page %>% 
      html_nodes("[class='is24qa-ausstattung text-content short-text']") %>%
      html_text() %>%
      str_trim() %>%
      str_replace_all(., "[^[:alnum:]]", " ") %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Lage
    lage <- page %>% 
      html_nodes("[class='is24qa-lage text-content short-text']") %>%
      html_text() %>%
      str_trim() %>%
      str_replace_all(., "[^[:alnum:]]", " ") %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Sonstiges
    sonstiges <- page %>% 
      html_nodes("[class='is24qa-sonstiges text-content short-text']") %>%
      html_text() %>%
      str_trim() %>%
      str_replace_all(., "[^[:alnum:]]", " ") %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Anbieter
    anbieter <- page %>% 
      html_nodes("[class='inline-block line-height-xs']") %>%
      html_text() %>%
      str_trim() %>%
      str_replace_all(., "[^[:alnum:]]", " ") %>%
      ifelse(identical(., character(0)),NA, .)
    
    
    # return data frame
    df <- data.frame(id_inserat,
                     kaufpreis,
                     kaltmiete,
                     warmmiete,
                     hausgeld,
                     provision,
                     typ,
                     etage,
                     wohnflaeche,
                     nutzflaeche, 
                     grundstück, 
                     bezugsfrei_ab,
                     zimmer, 
                     schlafzimmer, 
                     badezimmer, 
                     garage_stellplatz,
                     adresse, 
                     baujahr, 
                     energietraeger, 
                     objektbeschreibung,
                     ausstattung, 
                     lage, 
                     sonstiges, 
                     anbieter,
                     immo_url)
    
    df
  }
  
  proxy_list <- fn_get_proxies()
  
  
})

############
############
## KAUFEN ##
# WOHNUNG ##
############
############


# Ordner der gecrawlten Inserate
ordner_inserate <- "01_ordner_inserate"

# Ordner für Cluster Output - Ergebnise Suche maximale Anzahl an Ergebnisseiten
ordner_max_page <- "02_ordner_max_page"

# Ordner für Cluster Output - Inserate-ID, -Preis, -Link die in den Ergebnisseiten gefunden werden - 2021-05-29 18:37:30
ordner_ergebnisseiten <- "03_ordner_ergebnisseiten"

# Ordner der Stammdaten
ordner_stammdaten <- "04_ordner_stammdaten"


#### Stammdaten Laden ####
url_staedte_immo <- read.csv2(paste0(ordner_stammdaten,"/Links_ImmoScout.csv"))
# Nur Stuttgart
url_staedte_immo <- url_staedte_immo %>% filter(stadtname == "Stuttgart")

# Liste mit Urls der Ergebnisseiten erstellen
vec_urls <- NA
vec_name <- NA
for(i in 1:nrow(url_staedte_immo)){
  vec_urls <-c(vec_urls,paste0(url_staedte_immo$url_stadt[i],1:url_staedte_immo$max_page[i]))
  for(y in 1:url_staedte_immo$max_page[i]){
    vec_name <-c(vec_name,paste0(url_staedte_immo$stadtname[i]))
  }
}
vec_urls <- vec_urls[!is.na(vec_urls)]
vec_name <- vec_name[!is.na(vec_name)]

### Crawler 1 - Monitoring - 
# Ergebnisseiten durchsuchen, Inserate mit ID und Preis speichern und abgleichen, welche Inserate neu sein.
print(paste0("Es gibt ",length(vec_urls)," Ergebnisseiten zu durchsuchen."))

# Paralellisieren der Aufgaben - Ergebnisseite Durchsuchen und Inserate mit Infos in Datei speichern.
df_result <- foreach(i_cl = seq_along(vec_urls),
                     .combine=rbind,
                     .inorder=FALSE,
                     .errorhandling='pass',
                     .options.snow=opts,
                     .verbose = FALSE) %dopar% {
                       
                       tryCatch({
                         withTimeout({
                           
                           # Proxy nach 15 Minuten updaten
                           if((timer_proxy + 1000) < Sys.time()){
                             # Timer neu setzten
                             timer_proxy <- Sys.time()
                             # update proxy-list
                             proxy_list <- fn_get_proxies()} 
                           
                           # Page abrufen
                           page <- fn_get_page_content(vec_urls[i_cl], 50)
                           
                           # Wenn Page nicht leer ist
                           if(!is.null(page)){
                             
                             # Wenn erste Ergebnisseite, dann max_page auslesen
                             if(substrRight(vec_urls[i_cl], 1) == "1"){
                               max_page <- fn_scrape_immo_anzahl_seiten(page)
                               # wenn max_page nicht definiert werden konnte, sind es nur 4 oder weniger Ergebnisseiten
                               if(is.na(max_page) | max_page == ""){max_page <- 4}
                               max_page_df <- data.frame(vec_urls[i_cl],max_page) %>% mutate(Timestamp_scrape = Sys.time())
                               #save
                               file <- paste0(ordner_max_page,sprintf("/output_%d.csv" , Sys.getpid()))
                               write.table(max_page_df, file, sep = ",", col.names = !file.exists(file), append = T, row.names = F)
                             }
                             
                             # Alle Ergebnisseiten nach Inserate-ID, -Link und -Preis der Stadt durchsuchen.
                             df <- fn_ergebnisseiten_durchsuchen(page)
                             if(!is.null(df)){
                               # save
                               df <- df %>% mutate(stadtname = vec_name[i_cl], Timestamp_scrape = Sys.time())
                               file <- paste0(ordner_ergebnisseiten,sprintf("/output_%d.csv" , Sys.getpid()))
                               write.table(df, file, sep = ",", col.names = !file.exists(file), append = T, row.names = F)
                             }
                           }
                           
                         },timeout=150);
                       }, TimeoutException=function(ex) {return("Time Out!")})
                     }

#### Stammdaten aktualiseren ####

# Maximale Seite updaten
filelist <- list.files(path = ordner_max_page, recursive = TRUE,
                       pattern = "\\.csv$", 
                       full.names = TRUE)

#assuming tab separated values with a header    
datalist = lapply(filelist, function(x)read_delim(x,",", escape_double = FALSE)) 

#assuming the same header/columns for all files
max_page_df = do.call("rbind", datalist) 

# slice max
max_page_df <- max_page_df %>% group_by(vec_urls.i_cl.) %>% slice(which.max(Timestamp_scrape)) %>% ungroup()

max_page_df <- max_page_df %>%
  mutate(url_stadt = substr(vec_urls.i_cl., 1, nchar(vec_urls.i_cl.)-1)) 

url_staedte_immo <- url_staedte_immo %>%
  left_join(max_page_df, by=c("url_stadt")) %>%
  mutate(max_page = if_else(is.na(max_page.y),as.numeric(max_page.x), as.numeric(max_page.y))) %>%
  dplyr::select(stadtname, url_stadt, max_page)

url_staedte_immo <- url_staedte_immo %>% distinct()

# die maximale Anzahl an Ergebnisseiten wurde aktualisiert und wird gespeichert
write.csv2(url_staedte_immo,paste0(ordner_stammdaten,"/Links_ImmoScout.csv"), row.names = F)

# alte Files löschen
delfiles <- dir(path=ordner_max_page ,pattern="output*")
file.remove(file.path(ordner_max_page, delfiles))


# Abgleich mit vorhandenen Links - Alle gefundenen Links auslesen und mit allen gecrawlten Inseraten abgleichen

# Laden der gespeicherten Links
filelist <- list.files(path = ordner_ergebnisseiten, recursive = TRUE,
                       pattern = "\\.csv$", 
                       full.names = TRUE)

#assuming tab separated values with a header    
datalist = lapply(filelist, function(x)read_delim(x,",", escape_double = FALSE)) 

#assuming the same header/columns for all files
alle_links_df = do.call("rbind", datalist) 

# save merged file
write.table(alle_links_df, paste0(ordner_ergebnisseiten,"/merged_alle_links.csv"), sep = ",", row.names = F)

# alte Files löschen
delfiles <- dir(path=ordner_ergebnisseiten ,pattern="output*")
file.remove(file.path(ordner_ergebnisseiten, delfiles))



############
############
## KAUFEN ##
### HAUS ###
############
############

  
# Ordner der gecrawlten Inserate
ordner_inserate <- "01_ordner_inserate"
  
# Ordner für Cluster Output - Ergebnise Suche maximale Anzahl an Ergebnisseiten
ordner_max_page <- "02_ordner_max_page"
  
# Ordner für Cluster Output - Inserate-ID, -Preis, -Link die in den Ergebnisseiten gefunden werden - 2021-05-29 18:37:30
ordner_ergebnisseiten <- "03_ordner_ergebnisseiten"
  
# Ordner der Stammdaten
ordner_stammdaten <- "04_ordner_stammdaten"


#### Stammdaten Laden ####
url_staedte_immo <- read.csv2(paste0(ordner_stammdaten,"/Links_ImmoScout_Haus.csv"))

# Nur Stuttgart
url_staedte_immo <- url_staedte_immo %>% filter(stadtname == "Stuttgart")

# Liste mit Urls der Ergebnisseiten erstellen
vec_urls <- NA
vec_name <- NA
for(i in 1:nrow(url_staedte_immo)){
  vec_urls <-c(vec_urls,paste0(url_staedte_immo$url_stadt[i],1:url_staedte_immo$max_page[i]))
  for(y in 1:url_staedte_immo$max_page[i]){
    vec_name <-c(vec_name,paste0(url_staedte_immo$stadtname[i]))
  }
}
vec_urls <- vec_urls[!is.na(vec_urls)]
vec_name <- vec_name[!is.na(vec_name)]

### Crawler 1 - Monitoring - 
# Ergebnisseiten durchsuchen, Inserate mit ID und Preis speichern und abgleichen, welche Inserate neu sein.
print(paste0("Es gibt ",length(vec_urls)," Ergebnisseiten zu durchsuchen."))

# Paralellisieren der Aufgaben - Ergebnisseite Durchsuchen und Inserate mit Infos in Datei speichern.
df_result <- foreach(i_cl = seq_along(vec_urls),
                     .combine=rbind,
                     .inorder=FALSE,
                     .errorhandling='pass',
                     .options.snow=opts,
                     .verbose = FALSE) %dopar% {
                       
                       tryCatch({
                         withTimeout({
                           
                           # Proxy nach 15 Minuten updaten
                           if((timer_proxy + 1000) < Sys.time()){
                             # Timer neu setzten
                             timer_proxy <- Sys.time()
                             # update proxy-list
                             proxy_list <- fn_get_proxies()} 
                           
                           # Page abrufen
                           page <- fn_get_page_content(vec_urls[i_cl], 50)
                           
                           # Wenn Page nicht leer ist
                           if(!is.null(page)){
                             
                             # Wenn erste Ergebnisseite, dann max_page auslesen
                             if(substrRight(vec_urls[i_cl], 1) == "1"){
                               max_page <- fn_scrape_immo_anzahl_seiten(page)
                               # wenn max_page nicht definiert werden konnte, sind es nur 4 oder weniger Ergebnisseiten
                               if(is.na(max_page) | max_page == ""){max_page <- 4}
                               max_page_df <- data.frame(vec_urls[i_cl],max_page) %>% mutate(Timestamp_scrape = Sys.time())
                               #save
                               file <- paste0(ordner_max_page,sprintf("/output_%d.csv" , Sys.getpid()))
                               write.table(max_page_df, file, sep = ",", col.names = !file.exists(file), append = T, row.names = F)
                             }
                             
                             # Alle Ergebnisseiten nach Inserate-ID, -Link und -Preis der Stadt durchsuchen.
                             df <- fn_ergebnisseiten_durchsuchen(page)
                             if(!is.null(df)){
                               # save
                               df <- df %>% mutate(stadtname = vec_name[i_cl], Timestamp_scrape = Sys.time())
                               file <- paste0(ordner_ergebnisseiten,sprintf("/output_%d.csv" , Sys.getpid()))
                               write.table(df, file, sep = ",", col.names = !file.exists(file), append = T, row.names = F)
                             }
                           }
                           
                         },timeout=150);
                       }, TimeoutException=function(ex) {return("Time Out!")})
                     }

#### Stammdaten aktualiseren ####

# Maximale Seite updaten
filelist <- list.files(path = ordner_max_page, recursive = TRUE,
                       pattern = "\\.csv$", 
                       full.names = TRUE)

#assuming tab separated values with a header    
datalist = lapply(filelist, function(x)read_delim(x,",", escape_double = FALSE)) 

#assuming the same header/columns for all files
max_page_df = do.call("rbind", datalist) 

# slice max
max_page_df <- max_page_df %>% group_by(vec_urls.i_cl.) %>% slice(which.max(Timestamp_scrape)) %>% ungroup()

max_page_df <- max_page_df %>%
  mutate(url_stadt = substr(vec_urls.i_cl., 1, nchar(vec_urls.i_cl.)-1)) 

url_staedte_immo <- url_staedte_immo %>%
  left_join(max_page_df, by=c("url_stadt")) %>%
  mutate(max_page = if_else(is.na(max_page.y),as.numeric(max_page.x), as.numeric(max_page.y))) %>%
  dplyr::select(stadtname, url_stadt, max_page)

url_staedte_immo <- url_staedte_immo %>% distinct()

# die maximale Anzahl an Ergebnisseiten wurde aktualisiert und wird gespeichert
write.csv2(url_staedte_immo,paste0(ordner_stammdaten,"/Links_ImmoScout_Haus.csv"), row.names = F)

# alte Files löschen
delfiles <- dir(path=ordner_max_page ,pattern="output*")
file.remove(file.path(ordner_max_page, delfiles))


# Abgleich mit vorhandenen Links - Alle gefundenen Links auslesen und mit allen gecrawlten Inseraten abgleichen

# Laden der gespeicherten Links
filelist <- list.files(path = ordner_ergebnisseiten, recursive = TRUE,
                       pattern = "\\.csv$", 
                       full.names = TRUE)

#assuming tab separated values with a header    
datalist = lapply(filelist, function(x)read_delim(x,",", escape_double = FALSE)) 

#assuming the same header/columns for all files
alle_links_df = do.call("rbind", datalist) 

# save merged file
write.table(alle_links_df, paste0(ordner_ergebnisseiten,"/merged_alle_links.csv"), sep = ",", row.names = F)

# alte Files löschen
delfiles <- dir(path=ordner_ergebnisseiten ,pattern="output*")
file.remove(file.path(ordner_ergebnisseiten, delfiles))


############
############
# WOHNUNG ##
## MIETEN ##
############
############


# Ordner der gecrawlten Inserate
ordner_inserate <- "01_ordner_inserate"

# Ordner für Cluster Output - Ergebnise Suche maximale Anzahl an Ergebnisseiten
ordner_max_page <- "02_ordner_max_page_miete"

# Ordner für Cluster Output - Inserate-ID, -Preis, -Link die in den Ergebnisseiten gefunden werden - 2021-05-29 18:37:30
ordner_ergebnisseiten <- "03_ordner_ergebnisseiten_miete"

# Ordner der Stammdaten
ordner_stammdaten <- "04_ordner_stammdaten_miete"


#### Stammdaten Laden ####
url_staedte_immo <- read.csv2(paste0(ordner_stammdaten,"/Links_ImmoScout.csv"))

# Nur Stuttgart
url_staedte_immo <- url_staedte_immo %>% filter(stadtname == "Stuttgart")

# Liste mit Urls der Ergebnisseiten erstellen
vec_urls <- NA
vec_name <- NA
for(i in 1:nrow(url_staedte_immo)){
  vec_urls <-c(vec_urls,paste0(url_staedte_immo$url_stadt[i],1:url_staedte_immo$max_page[i]))
  for(y in 1:url_staedte_immo$max_page[i]){
    vec_name <-c(vec_name,paste0(url_staedte_immo$stadtname[i]))
  }
}
vec_urls <- vec_urls[!is.na(vec_urls)]
vec_name <- vec_name[!is.na(vec_name)]

### Crawler 1 - Monitoring - 
# Ergebnisseiten durchsuchen, Inserate mit ID und Preis speichern und abgleichen, welche Inserate neu sein.
print(paste0("Es gibt ",length(vec_urls)," Ergebnisseiten zu durchsuchen."))

# Paralellisieren der Aufgaben - Ergebnisseite Durchsuchen und Inserate mit Infos in Datei speichern.
df_result <- foreach(i_cl = seq_along(vec_urls),
                     .combine=rbind,
                     .inorder=FALSE,
                     .errorhandling='pass',
                     .options.snow=opts,
                     .verbose = FALSE) %dopar% {
                       
                       tryCatch({
                         withTimeout({
                           
                           # Proxy nach 15 Minuten updaten
                           if((timer_proxy + 1000) < Sys.time()){
                             # Timer neu setzten
                             timer_proxy <- Sys.time()
                             # update proxy-list
                             proxy_list <- fn_get_proxies()} 
                           
                           # Page abrufen
                           page <- fn_get_page_content(vec_urls[i_cl], 50)
                           
                           # Wenn Page nicht leer ist
                           if(!is.null(page)){
                             
                             # Wenn erste Ergebnisseite, dann max_page auslesen
                             if(substrRight(vec_urls[i_cl], 1) == "1"){
                               max_page <- fn_scrape_immo_anzahl_seiten(page)
                               # wenn max_page nicht definiert werden konnte, sind es nur 4 oder weniger Ergebnisseiten
                               if(is.na(max_page) | max_page == ""){max_page <- 4}
                               max_page_df <- data.frame(vec_urls[i_cl],max_page) %>% mutate(Timestamp_scrape = Sys.time())
                               #save
                               file <- paste0(ordner_max_page,sprintf("/output_%d.csv" , Sys.getpid()))
                               write.table(max_page_df, file, sep = ",", col.names = !file.exists(file), append = T, row.names = F)
                             }
                             
                             # Alle Ergebnisseiten nach Inserate-ID, -Link und -Preis der Stadt durchsuchen.
                             df <- fn_ergebnisseiten_durchsuchen(page)
                             if(!is.null(df)){
                               # save
                               df <- df %>% mutate(stadtname = vec_name[i_cl], Timestamp_scrape = Sys.time())
                               file <- paste0(ordner_ergebnisseiten,sprintf("/output_%d.csv" , Sys.getpid()))
                               write.table(df, file, sep = ",", col.names = !file.exists(file), append = T, row.names = F)
                             }
                           }
                           
                         },timeout=150);
                       }, TimeoutException=function(ex) {return("Time Out!")})
                     }

#### Ende, Stop Cluster ####

stopCluster(cl)

#### Stammdaten aktualiseren ####

# Maximale Seite updaten
filelist <- list.files(path = ordner_max_page, recursive = TRUE,
                       pattern = "\\.csv$", 
                       full.names = TRUE)

#assuming tab separated values with a header    
datalist = lapply(filelist, function(x)read_delim(x,",", escape_double = FALSE)) 

#assuming the same header/columns for all files
max_page_df = do.call("rbind", datalist) 

# slice max
max_page_df <- max_page_df %>% group_by(vec_urls.i_cl.) %>% slice(which.max(Timestamp_scrape)) %>% ungroup()

max_page_df <- max_page_df %>%
  mutate(url_stadt = substr(vec_urls.i_cl., 1, nchar(vec_urls.i_cl.)-1)) 

url_staedte_immo <- url_staedte_immo %>%
  left_join(max_page_df, by=c("url_stadt")) %>%
  mutate(max_page = if_else(is.na(max_page.y),as.numeric(max_page.x), as.numeric(max_page.y))) %>%
  dplyr::select(stadtname, url_stadt, max_page)

url_staedte_immo <- url_staedte_immo %>% distinct()

# die maximale Anzahl an Ergebnisseiten wurde aktualisiert und wird gespeichert
write.csv2(url_staedte_immo,paste0(ordner_stammdaten,"/Links_ImmoScout.csv"), row.names = F)

# alte Files löschen
delfiles <- dir(path=ordner_max_page ,pattern="output*")
file.remove(file.path(ordner_max_page, delfiles))


# Abgleich mit vorhandenen Links - Alle gefundenen Links auslesen und mit allen gecrawlten Inseraten abgleichen

# Laden der gespeicherten Links
filelist <- list.files(path = ordner_ergebnisseiten, recursive = TRUE,
                       pattern = "\\.csv$", 
                       full.names = TRUE)

#assuming tab separated values with a header    
datalist = lapply(filelist, function(x)read_delim(x,",", escape_double = FALSE)) 

#assuming the same header/columns for all files
alle_links_df = do.call("rbind", datalist) 

# save merged file
write.table(alle_links_df, paste0(ordner_ergebnisseiten,"/merged_alle_links.csv"), sep = ",", row.names = F)

# alte Files löschen
delfiles <- dir(path=ordner_ergebnisseiten ,pattern="output*")
file.remove(file.path(ordner_ergebnisseiten, delfiles))



############
############
# ABGLEICH #
############
############

# KAUFEN
kaufen_merged_alle_links <-  read_csv("03_ordner_ergebnisseiten/merged_alle_links.csv")

# MIETEN
mieten_merged_alle_links <-  read_csv("03_ordner_ergebnisseiten_miete/merged_alle_links.csv")

# rbind miete und kaufen
df_merged_alle_links <- rbind(mieten_merged_alle_links,kaufen_merged_alle_links)

# Duplikate raus
df_merged_alle_links <- df_merged_alle_links %>% distinct()

# den zusammengeführten DF speichern
write.table(df_merged_alle_links, "monitor_data.csv", sep = "~", row.names = F)

# nur die Links aus allen Links
alle_links_df <- df_merged_alle_links %>% dplyr::select(Inserate_ID) %>% distinct()

#####
# Laden der gespeicherten Inserate
inserate_df <- read.csv("IDs_Datenbank.csv")

# Als Vector zum Filtern
inserate_vec <- as.vector(inserate_df$id_inserat)


# Abgleich mit schon vorhandenen Links ##########
# Aus den neuen Links die alten herausfiltern
immo_links <- alle_links_df[!alle_links_df$Inserate_ID %in% inserate_vec, ] 
immo_links <- immo_links %>% mutate(Inserate_ID = paste0("https://www.immobilienscout24.de/expose/", Inserate_ID))
write.csv(immo_links, "immo_links.csv", row.names = F)
immo_links <- read.csv("immo_links.csv")
immo_links <- as.vector(immo_links$Inserate_ID)
print(paste0("Es sind ",length(immo_links)," neue Inserate gefunden."))

Sys.sleep(60)
##############
## Inserate ##
## Crawlen ###
##############

### set folders
# Ordner der gecrawlten Inserate
ordner_inserate <- "01_ordner_inserate"

# Ordner für Cluster Output - Ergebnise Suche maximale Anzahl an Ergebnisseiten
ordner_max_page <- "02_ordner_max_page"

# Ordner für Cluster Output - Inserate-ID, -Preis, -Link die in den Ergebnisseiten gefunden werden - 2021-05-29 18:37:30
ordner_ergebnisseiten <- "03_ordner_ergebnisseiten"

# Ordner der Stammdaten
ordner_stammdaten <- "04_ordner_stammdaten"

#### set up cluster ####
cl <- makeSOCKcluster(detectCores())
registerDoSNOW(cl)

# set options - log fertige tasks
progress <- function(n) cat(sprintf("task %d is complete\n", n))
opts <- list(progress=progress)

# initial each worker
clusterEvalQ(cl, {
  ### load library
  
  library(tidyverse)
  library(rvest)
  library(httr)
  library(R.utils)
  
  ### set variables
  timer_proxy <- Sys.time() - 1001
  
  ### load functions
  
  # Funktion get html 
  fn_get_page_content <- function(pushed_url, max_attempts) {
    # empty page and attempts
    page <- NULL
    attempt = 0
    # loop - mehrmals versuchen mit unterschiedlichen Proxy Seite aufzurufen
    for (attempt in 1:max_attempts) {
      if(is.null(page)){
        index_proxy <- round(runif(1, 1, length(proxy_list)),0)
        full_proxy <- paste0("http://",proxy_list[index_proxy])
        try(page <- read_html(httr::GET(url = pushed_url, httr::set_config(httr::use_proxy(full_proxy)))), silent=TRUE)
      }
    }
    return(page)
  } 
  
  # Anzahl der Ergebnisseiten ermitteln
  fn_scrape_immo_anzahl_seiten <- function(page) {
    # Maximale Anzahl an Ergebnis-Seiten
    max_page <- page %>% 
      html_node(css = "#listings > div > ul > li:nth-child(7) > a") %>%
      html_text() %>%
      str_trim() %>%
      ifelse(identical(., character(0)),NA, .)
    max_page
  }
  
  # Alle Ergebnisseiten nach Inserate-ID, -Link und -Preis der Stadt durchsuchen.
  fn_ergebnisseiten_durchsuchen <- function(page) {
    # IDs von den Inseraten
    Inserate_ID <- page %>% 
      html_nodes("article") %>% 
      html_attr("data-obid")
    if(!identical(Inserate_ID, character(0))){
      # Preis je Inserat über ID
      vec_preis <- c()
      for(i in 1:length(Inserate_ID)){
        preis_inserat <- page %>% 
          html_nodes(paste0("[data-obid='",Inserate_ID[i],"']")) %>%
          html_nodes("[class='font-highlight font-tabular']") %>%
          html_text() %>%
          str_trim() %>%
          ifelse(identical(., character(0)),NA, .)
        vec_preis <- c(vec_preis, preis_inserat)
      }
      return(data.frame(Inserate_ID, vec_preis))
    }
  }
  
  # Funktion get proxies
  fn_get_proxies <- function() {
    
    proxy_list_1 <- read.csv("https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt", header = FALSE)
    proxy_list_1 <- as.vector(proxy_list_1$V1)
    
    # Proxy-Liste von free-proxy-list.net
    proxy_list_2 <- read_html("https://free-proxy-list.net/") %>% 
      html_nodes("[class='form-control']") %>%
      html_text() %>%
      substr(., 76, nchar(.)-1) %>%
      str_replace_all("\\s+" , " ") %>%
      str_split(., " ") %>%
      unlist(.) 
    
    proxy_list <- c(proxy_list_1, proxy_list_2)
    print(paste0("Es stehen ",length(proxy_list), " Proxies zur Verfügung."))
    
    # give proxy_list back
    proxy_list
    
  }
  
  # Letzten Character aus String bekommen
  substrRight <- function(x, n){
    substr(x, nchar(x)-n+1, nchar(x))
  }
  
  # Funktion auslesen ImmobilienScout24 Anzeige ######################################
  fn_scrape_immo_inserat <- function(immo_url, page) {
    
    # id_inserat
    id_inserat <- gsub('https://www.immobilienscout24.de/expose/', '', immo_url) %>% ifelse(identical(., character(0)),NA, .)
    
    # Kaufpreis
    kaufpreis <- page %>% 
      html_nodes("[class='is24qa-kaufpreis grid-item three-fifths']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Kaltmiete
    kaltmiete <- page %>% 
      html_nodes("[class='is24qa-kaltmiete-main is24-value font-semibold is24-preis-value']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Warmmiete
    warmmiete <- page %>% 
      html_nodes("[class='is24qa-warmmiete-main is24-value font-semibold']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Hausgeld   
    hausgeld <- page %>% 
      html_nodes("[class='is24qa-hausgeld grid-item three-fifths']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Provision 
    provision <- page %>% 
      html_nodes("[class='is24qa-provision grid-item two-fifths']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Typ 
    typ <- page %>% 
      html_nodes("[class='is24qa-typ grid-item three-fifths']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Etage 
    etage <- page %>% 
      html_nodes("[class='is24qa-etage grid-item three-fifths']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Wohnflaeche  
    wohnflaeche <- page %>% 
      html_nodes("[class='is24qa-wohnflaeche-ca grid-item three-fifths']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Nutzflaeche
    nutzflaeche <- page %>% 
      html_nodes("[class='is24qa-nutzflaeche-ca grid-item three-fifths']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Grundstück 
    grundstück <- page %>% 
      html_nodes("[class='is24qa-grundstueck-ca grid-item three-fifths']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Bezugsfrei_ab
    bezugsfrei_ab <- page %>% 
      html_nodes("[class='is24qa-bezugsfrei-ab grid-item three-fifths']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Zimmer
    zimmer <- page %>% 
      html_nodes("[class='is24qa-zimmer grid-item three-fifths']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Schlafzimmer
    schlafzimmer <- page %>% 
      html_nodes("[class='is24qa-schlafzimmer grid-item three-fifths']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Badezimmer
    badezimmer <- page %>% 
      html_nodes("[class='is24qa-badezimmer grid-item three-fifths']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Garage_Stellplatz
    garage_stellplatz <- page %>% 
      html_nodes("[class='is24qa-garage-stellplatz grid-item three-fifths']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Adresse
    adresse <- page %>% 
      html_nodes("[class='zip-region-and-country']") %>%
      html_text() %>%
      str_trim() %>% 
      first() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Baujahr
    baujahr <- page %>% 
      html_nodes("[class='is24qa-baujahr grid-item three-fifths']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Energietraeger
    energietraeger <- page %>% 
      html_nodes("[class='is24qa-wesentliche-energietraeger grid-item three-fifths']") %>%
      html_text() %>%
      str_trim() %>%
      gsub('"', "", ., fixed=TRUE) %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Objektbeschreibung
    objektbeschreibung <- page %>% 
      html_nodes("[class='is24qa-objektbeschreibung text-content short-text']") %>%
      html_text() %>%
      str_trim() %>%
      str_replace_all(., "[^[:alnum:]]", " ") %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Ausstattung
    ausstattung <- page %>% 
      html_nodes("[class='is24qa-ausstattung text-content short-text']") %>%
      html_text() %>%
      str_trim() %>%
      str_replace_all(., "[^[:alnum:]]", " ") %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Lage
    lage <- page %>% 
      html_nodes("[class='is24qa-lage text-content short-text']") %>%
      html_text() %>%
      str_trim() %>%
      str_replace_all(., "[^[:alnum:]]", " ") %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Sonstiges
    sonstiges <- page %>% 
      html_nodes("[class='is24qa-sonstiges text-content short-text']") %>%
      html_text() %>%
      str_trim() %>%
      str_replace_all(., "[^[:alnum:]]", " ") %>%
      ifelse(identical(., character(0)),NA, .)
    
    # Anbieter
    anbieter <- page %>% 
      html_nodes("[class='inline-block line-height-xs']") %>%
      html_text() %>%
      str_trim() %>%
      str_replace_all(., "[^[:alnum:]]", " ") %>%
      ifelse(identical(., character(0)),NA, .)
    
    
    # return data frame
    df <- data.frame(id_inserat,
                     kaufpreis,
                     kaltmiete,
                     warmmiete,
                     hausgeld,
                     provision,
                     typ,
                     etage,
                     wohnflaeche,
                     nutzflaeche, 
                     grundstück, 
                     bezugsfrei_ab,
                     zimmer, 
                     schlafzimmer, 
                     badezimmer, 
                     garage_stellplatz,
                     adresse, 
                     baujahr, 
                     energietraeger, 
                     objektbeschreibung,
                     ausstattung, 
                     lage, 
                     sonstiges, 
                     anbieter,
                     immo_url)
    
    df
  }
  
  proxy_list <- fn_get_proxies()
  
  
})

# Abgleich mit schon vorhandenen Links ##########
# Aus den neuen Links die alten herausfiltern
immo_links <- read.csv("immo_links.csv")
immo_links <- as.vector(immo_links$Inserate_ID)
print(paste0("Es sind ",length(immo_links)," neue Inserate gefunden."))

### Crawler 2 - Inserate - 
# Neue noch nicht gecrawlte Inserate werden hier aufgerufen und gecrawlt.
df_result <- foreach(i_cl = seq_along(immo_links),
                     .combine=rbind,
                     .inorder=FALSE,
                     .errorhandling='pass',
                     .options.snow=opts,
                     .verbose = FALSE) %dopar% {
                       
                       tryCatch({
                         withTimeout({
                           # Seite aufrufen
                           page <- fn_get_page_content(immo_links[i_cl], 15)
                           if(!is.null(page)){
                             # HTML-Inserat nach Informationen durchsuchen
                             df <- fn_scrape_immo_inserat(immo_links[i_cl], page) 
                             
                             if(!is.null(df)){
                               df <- df %>% mutate(stadtname = "stadt", Timestamp_scrape = Sys.time())
                               #save
                               file <- paste0(ordner_inserate,sprintf("/output_%d.csv" , Sys.getpid()))
                               write.table(df, file, sep = "~", col.names = !file.exists(file), append = T, row.names = F)
                             } 
                           } 
                           
                         },timeout=150); ### Cumulative Timeout for entire process
                       }, TimeoutException=function(ex) {return("Time Out!")})
                       
                     }

#### Ende, Stop Cluster ####

stopCluster(cl)

#### Abschließendes Zusammenführen der neuen Inserate in ein File ####

# Laden der gespeicherten Inserate
filelist <- list.files(path = ordner_inserate, recursive = TRUE,
                       pattern = "\\.csv$", 
                       full.names = TRUE)

#assuming tab separated values with a header    
datalist = lapply(filelist, function(x)read_delim(x,"~", escape_double = FALSE, col_types = cols(
  .default = col_character(),
  id_inserat = col_double(),
  kaltmiete = col_character(),
  warmmiete = col_character(),
  grundstück = col_logical(),
  zimmer = col_character(),
  schlafzimmer = col_double(),
  badezimmer = col_double(),
  Timestamp_scrape = col_datetime(format = "")
))) 

#assuming the same header/columns for all files
inserate_df = do.call("rbind", datalist) 

# Duplikate raus
inserate_df <- inserate_df %>% distinct()

# Filter - Inserate ihne Url - Wahrscheinlich sind hier Fehler aufgetreten
inserate_df <- inserate_df %>% filter(!is.na(immo_url))

# den zusammengeführten DF speichern
write.table(inserate_df, paste0(ordner_inserate,"/04_merged_immoscout.csv"), sep = "~", row.names = F)

# Nur Links speichern
inserate_df_nur_IDs <- inserate_df %>% select(id_inserat) %>% distinct()
write.csv(inserate_df_nur_IDs, "IDs_Datenbank.csv", row.names = F)

# alte Files löschen
delfiles <- dir(path=ordner_inserate ,pattern="output*")
file.remove(file.path(ordner_inserate, delfiles))
