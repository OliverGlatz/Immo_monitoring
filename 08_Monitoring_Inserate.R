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
