### Przetwarzanie Danych Ustrukturyzowanych 2023L
### Praca domowa nr. 3
###
### UWAGA:
### nazwy funkcji oraz ich parametrow powinny pozostac niezmienione.
###  
### Wskazane fragmenty kodu przed wyslaniem rozwiazania powinny zostac 
### zakomentowane
###

# -----------------------------------------------------------------------------#
# Wczytanie danych oraz pakietow.
# !!! Przed wyslaniem zakomentuj ten fragment
# -----------------------------------------------------------------------------#
# Posts <- read.csv("Posts.csv.gz")
# Users <- read.csv("Users.csv.gz")
# Comments <- read.csv("Comments.csv.gz")
# install.packages("sqldf")
# install.packages("dplyr")
# install.packages("data.table")
# install.packages("microbenchmark")
# install.packages("compare")
# library(sqldf)
# library(dplyr)
# library(data.table)
# library(microbenchmark)
# library(compare)

# -----------------------------------------------------------------------------#
# Zadanie 1
# -----------------------------------------------------------------------------#

sql_1 <- function(Users){
  wynik1 <- sqldf("
  SELECT Location, SUM(UpVotes) as TotalUpVotes
  FROM Users
  WHERE Location != ''
  GROUP BY Location
  ORDER BY TotalUpVotes DESC
  LIMIT 10
                  ")
}

base_1 <- function(Users){
  funkcjeb1 <- Users[Users$Location != "",] #Usuwamy wiersze z pusta lokalizacja
  funkcjeb1 <- aggregate(list(TotalUpVotes= funkcjeb1$UpVotes), funkcjeb1["Location"], FUN = sum) #Sumujemy wszystkie glosy dla danych lokalizacij od razu nadajac odpowiednia nazwe otrzymywanej kolumnie 
  funkcjeb1 <- funkcjeb1[ order(funkcjeb1$TotalUpVotes, decreasing = TRUE), ] #Sortujemy glosy z TotalUpVotes malejaco
  funkcjeb1 <- funkcjeb1[1:10, ] #Wybieramy pierwszych 10 wierszy
  row.names(funkcjeb1) <- NULL #"resetujemy" numery wierszy
}


dplyr_1 <- function(Users){
  funkcjedp <- filter(Users, Location != "") #Usuwamy wiersze z pusta lokalizacja
  funkcjedp <-  group_by(funkcjedp, Location) #Grupujemy dane wedlug lokalizacji
  funkcjedp <- summarise(funkcjedp, TotalUpVotes = sum(UpVotes)) #Sumujemy glosy dla kazdego miasta
  funkcjedp <- arrange(funkcjedp, desc(TotalUpVotes)) #Sortujemy dane malejaco ze wzgledu na glosy w TotalUpVotes
  funkcjedp <- slice(funkcjedp, 1:10) #Wybieramy pierwsze 10 wierszy
}

table_1 <- function(Users){
  tabelka <- data.table(Users) #Zamieniamy ramke danych na typ data.table
  tabelka <- tabelka[Location != ""] #Usuwamy wiersze z pusta lokalizacja
  tabelka <- tabelka[, .(TotalUpVotes = sum(UpVotes)), by = Location] #Tworzymy nowa kolumna bedaca suma glosow (UpVotes) ze wzgledu na lokalizacja (Location)
  tabelka <- setorder(tabelka, -TotalUpVotes) #Sortujemy wiersze ze wzgledu na glosy z TotaUpVotes malejaco
  tabelka <- tabelka[1:10, ] #Wybieramy pierwsze 10 wierszy
}
# Sprawdzenie rownowaznosci wynikow - zakomentuj te czesc przed wyslaniem
  # all_equal(funkcjeb1, wynik1)
  # all_equal(funkcjedp, wynik1)
  # all_equal(tabelka, wynik1)

# Porowanie czasow wykonania - zakomentuj te czesc przed wyslaniem
    # porownanie1 <- microbenchmark(
    # sqldf = sql_1(Users),
    # base = base_1(Users),
    # dplyr = dplyr_1(Users),
    # data.table = table_1(Users))
# Wynik
# Unit: milliseconds
# expr      min        lq      mean    median        uq      max neval
# sqldf 166.7050 167.87630 176.07594 170.60800 179.24470 220.9074   100
# base 115.2836 117.05855 126.25718 118.97970 126.62440 181.8093   100
# dplyr  27.2222  28.22820  32.65127  28.70145  30.20530  87.1045   100
# data.table  14.5978  16.23035  20.69292  16.69910  17.44465  68.6860   100

# -----------------------------------------------------------------------------#
# Zadanie 2
# -----------------------------------------------------------------------------#

sql_2 <- function(Posts){
  wynik2 <- sqldf("
  SELECT STRFTIME('%Y', CreationDate) AS Year, STRFTIME('%m', CreationDate) AS Month,
          COUNT(*) AS PostsNumber, MAX(Score) AS MaxScore
  FROM Posts
  WHERE PostTypeId IN (1, 2)
  GROUP BY Year, Month
  HAVING PostsNumber > 1000
")
}

base_2 <- function(Posts){
  bazowe2 <- Posts[Posts$PostTypeId == 1 | Posts$PostTypeId == 2,] #Usuwamy wiersze z ID roznym od 1 i roznym od 2
  bazowe2$CreationDate <- as.Date(bazowe2$CreationDate) #Zamieniamy kolumne z data na typ Date
  bazowe2 <- transform(bazowe2, Year = format(CreationDate, "%Y"), Month = format(CreationDate, "%m")) #Tworzymy kolumny z rokiem i miesiacem za pomoca formatowania, ktorego moglizmy uzyc dzieki zamianie na typ Date
  PostNumber <- aggregate(bazowe2$PostTypeId, bazowe2[c("Year", "Month")] , FUN = length) #Liczymy posty w danym miesiacu i roku 
  MaxScore <- aggregate(bazowe2$Score, bazowe2[c("Year", "Month")] , FUN = max) #Liczymy maksymalny wynik postu w danym miesiacu i roku
  wynikb2 <- cbind(PostNumber, MaxScore[,3]) #Laczymy macierz PostNumber z 3 kolumna MaxScore aby otrzymac chciany wynik
  wynikb2 <- wynikb2[wynikb2$x > 1000, ] #Usuwamy wiersze z PostNumber <=1000
  colnames(wynikb2) <- c("Year", "Month", "PostsNumber", "MaxScore") #Nadajemy nazwy jak w przykladzie
  #Kosmetyczne zmiany aby otrzymac kolejnosc wierszy jak w przykladzie
  wynikb2 <- wynikb2[ order(wynikb2$Year, decreasing = FALSE), ] #Sortujemy lata rosnaco
  rownames(wynikb2) <- NULL #"Resetujemy" numery wierszy
}

dplyr_2 <- function(Posts){
  funkcjedp2 <- filter(Posts, PostTypeId == 1 | PostTypeId == 2) #Wybieramy wiersze z PostTypeId = 1 lub = 2
  funkcjedp2$CreationDate <- as.Date(funkcjedp2$CreationDate) #Zamieniamy dane w kolumnie CreationDate na typa Date
  funkcjedp2 <- mutate(funkcjedp2, Year = format(funkcjedp2$CreationDate, "%Y"), Month = format(funkcjedp2$CreationDate, "%m")) #Tworzymy nowe kolumne Year i Month uzyskane z kolumny CreationDate
  funkcjedp2 <- group_by(funkcjedp2, Year, Month) #Grupujemy dane po roku i miesiacu
  funkcjedp2 <- summarise(funkcjedp2, PostsNumber = length(PostTypeId) , MaxScore = max(Score)) #Tworzymy 2 nowe kolumny PostNumber i MaxScore bedace odpowiednio iloscia PostTypeId i maksymalnym wynikiem punktow(Score) ze wzgledu na poprzednie grupowanie
  funkcjedp2 <- filter(funkcjedp2, PostsNumber > 1000) #Wybieramy te wiersze w ktorych PostNumber > 1000
}

table_2 <- function(Posts){
  tabela2 <- data.table(Posts) #Zamieniamy ramke danych Posts na typ data.table
  tabela2 <- tabela2[PostTypeId == 1 | PostTypeId == 2] #Wybieramy wiersze, w ktorych PostyType = 1 lub = 2 
  tabela2 <- tabela2[, c("Year", "Month", "day") := tstrsplit(as.Date(tabela2$CreationDate), "-")]
  tabela2 <- tabela2[, .(PostsNumber = length(PostTypeId), MaxScore = max(Score)), by = .(Year, Month)] #Tworzymy 2 nowe kolumny PostNumber i MaxScore bedace odpowiednio iloscia PostTypeId i maksymalnym wynikiem punktow(Score) grupujac je po roku i miesiacu
  tabela2 <- tabela2[PostsNumber > 1000] #Wybieramy te wiersze w ktorych PostsNumber > 1000
}

# Sprawdzenie rownowaznosci wynikow - zakomentuj te czesc przed wyslaniem
# compare(wynikb2, wynik2, allowAll = TRUE)
# compare(funkcjedp2, wynik2, allowAll = TRUE)
# compare(tabela2, wynik2, allowAll = TRUE)

# Porowanie czasow wykonania - zakomentuj te czesc przed wyslaniem
# porownanie2 <- microbenchmark(
#   sqldf = sql_2(Posts),
#   base = base_2(Posts),
#   dplyr = dplyr_2(Posts),
#   data.table = table_2(Posts))

# Wynik
# Unit: milliseconds
# expr       min       lq     mean   median       uq      max neval
# sqldf 1005.1213 1010.663 1041.756 1013.502 1057.871 1285.765   100
# base 1098.5003 1151.441 1174.332 1156.895 1172.903 1473.020   100
# dplyr  996.3676 1002.832 1044.766 1045.946 1055.510 1350.231   100
# data.table  957.3224 1012.749 1049.468 1022.709 1047.088 1437.861   100


# -----------------------------------------------------------------------------#
# Zadanie 3
# -----------------------------------------------------------------------------#

sql_3 <- function(Posts, Users){
  Questions <- sqldf( 'SELECT OwnerUserId, SUM(ViewCount) as TotalViews
                                      FROM Posts
                                      WHERE PostTypeId = 1
                                      GROUP BY OwnerUserId' )
  wynik3 <- sqldf( "SELECT Id, DisplayName, TotalViews
                FROM Questions
                JOIN Users
                ON Users.Id = Questions.OwnerUserId
                ORDER BY TotalViews DESC
                LIMIT 10")
}

base_3 <- function(Posts, Users){
  Questions <- Posts[c("OwnerUserId", "ViewCount", "PostTypeId")] #Wybieramy odpowiednie kolumny z ramki Posts
  Questions <- Questions[Questions$PostTypeId == 1, ] #Usuwamy wiersze z PostTypeId roznym od 1
  Questions <- aggregate(list(TotalViews = Questions$ViewCount), by = Questions["OwnerUserId"], FUN = sum) #Tworzymy nowa kolumne bedaco suma ViewCount ze wzedu na OwnerUserId od razu nadajac odpowiednie nazwy kolumnom  
  ostatecznie <- merge(Users[c("Id", "DisplayName")], Questions, by.x = "Id", by.y = "OwnerUserId") #Laczymy kolumny Id i DisplayName z ramki Users z ramka Questions, tak ze Id z Users = OwnerUserId z Qesutions
  ostatecznie <- ostatecznie[ order(ostatecznie$TotalViews, decreasing = TRUE),] #Sortujemy dane malejaco ze wzgledu na TotalViews
  ostatecznie <- ostatecznie[1:10, ] #Wybieramy pierwsze 10 wierszy
  row.names(ostatecznie) <- NULL #"Resetujemy" numery wierszy
}

dplyr_3 <- function(Posts, Users){
    funkcjedp3 <- select(Posts, OwnerUserId, ViewCount, PostTypeId) #Wybieramy odpowiednie kolumny z ramki Posts
    funkcjedp3 <- filter(funkcjedp3, PostTypeId == 1, !is.na(OwnerUserId)) #Usuwamy wiersze z PostTypeId = 1 i wiersze w ktorych OwnerUserId = NA (Zeby nie zliczalo nam sumy gdy nie znamy Id)
    funkcjedp3 <- group_by(funkcjedp3, OwnerUserId) #Grupujemy dane ze wzgledu na OwnerUserId
    funkcjedp3 <- summarize(funkcjedp3, TotalViews = sum(ViewCount)) #Tworzymy nowa kolumne o nazwie TotalViews bedace suma ViewCount ze wzgledu na poprzednie grupowanie
    funkcjedp3 <- inner_join(funkcjedp3, Users, by = c("OwnerUserId" = "Id")) #Laczymy ramki danych funkcjedp3 i Users tak ze OwnerUserId = Id
    funkcjedp3 <- select(funkcjedp3, OwnerUserId, DisplayName, TotalViews) #Wybieramy odpowiednie kolumny 
    funkcjedp3 <- arrange(funkcjedp3, desc(TotalViews)) #Sortujemy dane malejaco ze wzledu na TotalViews
    funkcjedp3 <- rename(funkcjedp3, Id = OwnerUserId) #Zmieniamy nazwe kolumny z Id na OwnerUserId
    funkcjedp3 <- slice(funkcjedp3, 1:10) #Wybieramy pierwsze 10 wierszy
}

table_3 <- function(Posts, Users){
  tabelka3 <- data.table(Posts) #Zamieniamy ramke danych Posts na data.table
  tabelka3 <- tabelka3[PostTypeId == 1, .(OwnerUserId, ViewCount)] #Wybieramy wiersze gdzie PostTypeId = 1 i odpowiednie kolumny
  tabelka3 <- tabelka3[, .(TotalViews = sum(ViewCount)), by = OwnerUserId] #Tworzymy nowa kolumne bedaco suma ViewCount grupowane ze wzgledu na OwnerUserId, jednoczsnie nadajac odpowiednia nazwe kolumnie
  tabelka3 <- merge(as.data.table(Users[, c("DisplayName", "Id")]), tabelka3,  by.y = "OwnerUserId", by.x = "Id") #Laczymy zamienino na data.table ramke danych users z nasza tabelka tak ze OwnerUserId = Id
  tabelka3 <- tabelka3[order(-TotalViews)] #Sortujemy dane malejaco ze wzgledu na TotalViews
  tabelka3 <- tabelka3[1:10] #Wybieramy pierwsze 10 wierszy
}

# Sprawdzenie rownowaznosci wynikow - zakomentuj te czesc przed wyslaniem
# compare(ostatecznie, wynik3, allowAll = TRUE)
# compare(wynik3,funkcjedp3, allowAll = TRUE)
# compare(tabelka3, wynik3, allowAll = TRUE)

# Porowanie czasow wykonania - zakomentuj te czesc przed wyslaniem
# porownanie3 <- microbenchmark(
#   sqldf = sql_3(Posts, Users),
#   base = base_3(Posts, Users),
#   dplyr = dplyr_3(Posts, Users),
#   data.table = table_3(Posts, Users), times = 100)
# Wynik
# Unit: milliseconds
# expr       min        lq       mean     median         uq       max neval
# sqldf 1126.2569 1131.7242 1150.65731 1138.61965 1168.74740 1286.9735   100
# base  263.1045  265.7940  285.95860  267.94160  306.71025  501.3246   100
# dplyr   54.5277   56.8729   71.30746   58.87385   96.33165  116.5482   100
# data.table   18.0715   20.1001   45.36959   26.48110   68.48950  228.6976   100


# -----------------------------------------------------------------------------#
# Zadanie  4
# -----------------------------------------------------------------------------#

sql_4 <- function(Posts, Users){
 wynik4 <- sqldf("
  SELECT DisplayName, QuestionsNumber, AnswersNumber, Location, Reputation, UpVotes, DownVotes
  FROM (
          SELECT *
          FROM (
                  SELECT COUNT(*) as AnswersNumber, OwnerUserId
                  FROM Posts
                  WHERE PostTypeId = 2
                  GROUP BY OwnerUserId
          ) AS Answers
          JOIN
              (
                  SELECT COUNT(*) as QuestionsNumber, OwnerUserId
                  FROM Posts
                  WHERE PostTypeId = 1
                  GROUP BY OwnerUserId
              ) AS Questions
          ON Answers.OwnerUserId = Questions.OwnerUserId
          WHERE AnswersNumber > QuestionsNumber
          ORDER BY AnswersNumber DESC
          LIMIT 5
        ) AS PostsCounts
  JOIN Users
  ON PostsCounts.OwnerUserId = Users.Id")
}

base_4 <- function(Posts, Users){
  Answers <- Posts[Posts$PostTypeId == 2,] #Wybieramy wiersze z PostTypeId = 2 
  Answers <- aggregate(Answers$AnswerCount, Answers["OwnerUserId"], FUN = length) #Tworzymy nowa kolumne zliczajaca AnswerCount ze wzgledu na OwnerUserId
  colnames(Answers)[2] <- "AnswersNumber" #Nadajmey nazwe 2 kolumnie
  Questions <- Posts[Posts$PostTypeId == 1,] #Wybieramy wiersze z PostTypeId = 1
  Questions <- aggregate(Questions$AnswerCount, by = Questions["OwnerUserId"], FUN = length) #Tworzymy nowa kolumne zliczajaca AnswerCount ze wzgledu na OwnerUserId
  colnames(Questions)[2] <- "QuestionsNumber" #Nadajemy nazwe 2 kolumnie
  PostsCounts <- merge(Answers, Questions,  by = "OwnerUserId") #Laczymy wszescniej utworzone ramki po OwnerUserId
  PostsCounts <- PostsCounts[PostsCounts$AnswersNumber > PostsCounts$QuestionsNumber,] #Wybieramy wiersze w ktorych AnswerNumber > QuestionsNumber
  PostsCounts <- PostsCounts[order(PostsCounts$AnswersNumber, decreasing = TRUE),] #Sortujemy dane malejaco ze wzgledu na AnswersNumber
  PostsCounts <- PostsCounts[1:5, ] #Wybieramy pierwsze 10 wierszy
  wynikb4 <- merge(PostsCounts, Users, by.x = "OwnerUserId", by.y = "Id", sort = FALSE) #Laczymy ramki PostsCounts i Users po OnwerUser = Idi zachowujemy nasze wczesniejsze sortowanie
  wynikb4 <- wynikb4[,c("DisplayName", "QuestionsNumber", "AnswersNumber", "Location", "Reputation", "UpVotes", "DownVotes")] #Wybieramy odpowiednie kolumny
  
}

dplyr_4 <- function(Posts, Users){
  Answers <- filter(Posts, PostTypeId == 2, !is.na(OwnerUserId)) #Wybieramy z ramki Posts wiersze dla ktorych PostTypeId = 2 i OwnerUserId rozne od NA(zeby nie liczyc ilosci dla nieznanych uzytkownikow)
  Answers <- group_by(Answers, OwnerUserId) #Grupujemy dane z Answers po OwnerUserId
  Answers <- summarize(Answers, AnswersNumber = length(AnswerCount)) #Dodajemy kolumne AnswesrNumber z iloscia AnswerCount liczona ze wzgledu na wczesniejsze grupowanie
  Questions <- filter(Posts, PostTypeId == 1, !is.na(OwnerUserId)) #Wybieramy z ramki Posts wiersze dla ktorych PostTypeId = 1 i OwnerUserId rozne od NA(zeby nie liczyc ilosci dla nieznanych uzytkownikow)
  Questions <- group_by(Questions, OwnerUserId) #Grupujemy dane z Quesions ze wzgledu na OwnerUserId
  Questions <- summarize(Questions, QuestionsNumber = length(AnswerCount)) #Dodajemy kolumne QeustionsNumber z iloscia AnswerCount liczona ze wzgledu na wczesniejsze grupowanie
  PostsCounts <- inner_join(Questions, Answers, by = "OwnerUserId") #Laczymy wczesniej utworzone ramki danych po OwnerUserId
  PostsCounts <- filter(PostsCounts, AnswersNumber > QuestionsNumber) #Wybieramy wiersze, w ktorych AnswersNumber > QuestionsNumber
  PostsCounts <- arrange(PostsCounts, desc(AnswersNumber)) #Sortujemy dane malejaco po AnswerNumber
  PostsCounts <- slice(PostsCounts, 1:5) #Wybieramy pierwsze 5 wierszy
  wynikd4 <- inner_join(PostsCounts, Users, by = join_by(OwnerUserId == Id)) #Laczymy otrzymana wczesniej ramke z ramka Users, tak ze OwnerUserId = Id
  wynikd4 <- select(wynikd4, DisplayName, QuestionsNumber, AnswersNumber, Location, Reputation, UpVotes, DownVotes) #Wybieramy odpowiednie kolumny
}

table_4 <- function(Posts, Users){
  dt_posts <- data.table(Posts) #Tworzymy pomocnicza tabelka typu data.table z ramki Posts
  Answers <- dt_posts[PostTypeId == 2 & !is.na(OwnerUserId), .(AnswersNumber = .N), by = OwnerUserId] #Zostawiamy wiersze z PostTypeId = 2 oraz OwnerUserId roznym od NA, jednoczesnie tworzymy nowa kolumne bedaca iloscia pytan (AnswersNumber) ze wzgledu na OwnerUserId
  Questions <- dt_posts[PostTypeId == 1 & !is.na(OwnerUserId), .(QuestionsNumber = .N), by = OwnerUserId] #Zostawiamy wiersze z PostTypeId = 1 oraz OwnerUserId roznym od NA, jednoczesnie tworzymy nowa kolumne bedaca iloscia odpowiedzi (QuestionesNumber) ze wzgledu na OwnerUserId
  PostsCounts <- Answers[Questions, on = "OwnerUserId"] #Laczymy tabele Answer i Questions ze wzgledu na OwnerUserId
  PostsCounts <- PostsCounts[AnswersNumber > QuestionsNumber] #Zostawiamy tylko wiersze, gdzie AnswersNumber > QuestionsNumber
  PostsCounts <- setorder(PostsCounts, -AnswersNumber) #Sortujemy dane malejaco po AnswerNumber
  PostsCounts <- PostsCounts[1:5, ] #Wybieramy 5 pierwszych wierszy
  wynikt4 <- PostsCounts[data.table(Users), on = c("OwnerUserId" = "Id"), nomatch = 0][order(PostsCounts)] #Laczymy powstala tablke z ramka (zamieniona jednoczsnie na tabelke) Users, dopasowujac po OwnerUserId = Id i zachowujac kolejnosc z PostCounts
  wynikt4 <- wynikt4[, .(DisplayName, QuestionsNumber, AnswersNumber, Location, Reputation, UpVotes, DownVotes)] #Wybieramy odpowiednia kolumne
}

# Sprawdzenie rownowaznosci wynikow - zakomentuj te czesc przed wyslaniem
# compare(wynik4, wynikb4, allowAll = TRUE)
# compare(wynik4, wynikd4, allowAll = TRUE)
# compare(wynik4, wynikt4, allowAll = TRUE)

# Porowanie czasow wykonania - zakomentuj te czesc przed wyslaniem
# porownanie4 <- microbenchmark(
#   sqldf = sql_4(Posts, Users),
#   base = base_4(Posts, Users),
#   dplyr = dplyr_4(Posts, Users),
#   data.table = table_4(Posts, Users))
# Wynik
# Unit: milliseconds
# expr       min        lq      mean     median        uq       max neval
# sqldf 1248.2855 1323.8883 1375.8313 1360.36825 1428.9517 1666.8897   100
# base  395.6704  450.5601  474.4734  470.00915  495.4411  696.2911   100
# dplyr   89.3373  108.5456  143.0903  146.97005  158.8257  354.7170   100
# data.table   23.2992   28.9144   58.6716   41.47825   83.6595  346.3844   100

# -----------------------------------------------------------------------------#
# Zadanie 5 
# -----------------------------------------------------------------------------#

sql_5 <- function(Posts, Comments, Users){
  CmtTotScr <- sqldf('SELECT PostId, SUM(Score) AS CommentsTotalScore
                        FROM Comments
                        GROUP BY PostId')
  PostsBestComments <- sqldf('
        SELECT Posts.OwnerUserId, Posts.Title, Posts.CommentCount, Posts.ViewCount, 
               CmtTotScr.CommentsTotalScore
        FROM CmtTotScr
        JOIN Posts ON Posts.Id = CmtTotScr.PostId
        WHERE Posts.PostTypeId=1')
  
  wynik <- sqldf( 'SELECT Title, CommentCount, ViewCount, CommentsTotalScore, DisplayName, Reputation, Location
                 FROM PostsBestComments
                 JOIN Users ON PostsBestComments.OwnerUserId = Users.Id
                 ORDER BY CommentsTotalScore DESC
                 LIMIT 10' )
}

base_5 <- function(Posts, Comments, Users){
    CmTotScr <- aggregate(Comments$Score, Comments["PostId"], FUN = sum) #Tworzymy nowa kolumne z suma punktow (Score) grupujac ze wzgledu na PostId
    colnames(CmTotScr)[2] <- "CommentsTotalScore" #Nadalejmy nazwe drugiej kolumnie
    PostsBestComments <- Posts[Posts$PostTypeId == 1,] #Wybieramy wiersze gdzie PostTypeId = 1
    PostsBestComments <- PostsBestComments[,c("Id","OwnerUserId", "Title", "CommentCount", "ViewCount")] #Wybieramy odpowiednie kolumny
    PostsBestComments <- merge(PostsBestComments, CmTotScr, by.x = "Id", by.y = "PostId") #Laczymy ramki PostBestComments i CmTotScr po Id = PostId
    wynikb5 <- merge(Users, PostsBestComments, by.x = "Id", by.y = "OwnerUserId") #Laczymy ramki Users i PostsBestComments po id = OwnerUserId 
    wynikb5 <- wynikb5[, c("Title", "CommentCount", "ViewCount", "CommentsTotalScore", "DisplayName", "Reputation", "Location")] #Wybieramy odpowiednie kolumny
    wynikb5 <- wynikb5[order(wynikb5$CommentsTotalScore, decreasing =  TRUE), ] #Sortujemy dane malejaco ze wzledu na CommentsTotalScore
    wynikb5 <- wynikb5[1:10, ] #Wybieramy pierwsze 10 wierszy z ramki
    rownames(wynikb5) <- NULL #"Resetujemy" numery wierszy
}

dplyr_5 <- function(Posts, Comments, Users){
    CmTotScr <- select(Comments, PostId, Score) #Wybieramy odpowiednie kolumny z ramki Comments
    CmTotScr <- group_by(CmTotScr, PostId) #Grupujemy dane wzgledem PostId
    CmTotScr <- summarise(CmTotScr, CommentsTotalScore = sum(Score)) #Tworzymy nowa kolumne, w kotrej bedziemy miec sume punktow(Score) ze wzgledu na wczesniej zgrupowanie PostId
    PostsBestComments <- filter(Posts, PostTypeId == 1) #Wybieramy wiersze gdzie PostTypeId = 1
    PostsBestComments <- select(PostsBestComments, Id, OwnerUserId, Title, CommentCount, ViewCount) #Wybieramy odpowiednie kolumny z ramki PostBestComments
    PostsBestComments <- inner_join(PostsBestComments, CmTotScr, by = join_by(Id == PostId)) #Laczymy ramki PostBestComments z CmTotScr po Id = OwnerUserId
    wynikd5 <- inner_join(Users, PostsBestComments, by = join_by(Id == OwnerUserId)) #Laczymy ramki Users i PostBestComments po Id = OwnerUserId
    wynikd5 <- select(wynikd5, Title, CommentCount, ViewCount, CommentsTotalScore, DisplayName, Reputation, Location) #Wybieramy podane kolumny z ramki
    wynikd5 <- arrange(wynikd5, desc(CommentsTotalScore)) #Sortujemy dane malejaco
    wynikd5 <- slice(wynikd5, 1:10) #wybieramy 10 pierwszych wierszy
}

table_5 <- function(Posts, Comments, Users){
    CmTotScr1 <- data.table(Comments) #Przeksztalcamy ramke danych na obiekt data.table
    CmTotScr1 <- CmTotScr1[, .(PostId, Score)] #Wybieramy odpowiednie kolumny
    CmTotScr1 <- CmTotScr1[, .(CommentsTotalScore = sum(Score)), by = PostId] #Tworzymy kolumna sumujaca wynik ze wzledu na PostId
    PostsBestComments <- data.table(Posts) #Przeksztalcamy ramke danych na obiekt data.table
    PostsBestComments <- PostsBestComments[PostTypeId == 1,] #Wybieramy tylko te wiersze w ktorych PostTypeId = 1
    PostBestComments <- PostsBestComments[, .(Id,OwnerUserId, Title, CommentCount, ViewCount)] #Wybieramy odpowiednie kolumny
    PostsBestComments <- PostsBestComments[CmTotScr1, on = c(Id = "PostId"), nomatch = 0] #Laczymy ze soba tebele PostBestComments i CmTotScr1 ze wzgledu na id
    wynikt5 <- PostsBestComments[data.table(Users), on = c(OwnerUserId = "Id"), nomatch = 0] #Laczymy ze soba tabele PostBestCommets i Users rowniez ze wzgledu na id
    wynikt5 <- wynikt5[, .(Title, CommentCount, ViewCount, CommentsTotalScore, DisplayName, Reputation, Location)] #Wybieramy odpowiednie kolumny
    wynikt5 <- setorder(wynikt5, -CommentsTotalScore) #Sortujemy dane malejaco ze wzledu na CommentsTotalScore
    wynikt5 <- wynikt5[1:10, ] #Wybieramy pierwsze 10 wierszy
}

# Sprawdzenie rownowaznosci wynikow - zakomentuj te czesc przed wyslaniem
# compare(wynik, wynikb5, allowAll = TRUE)
# compare(wynik, wynikd5, allowAll = TRUE)
# compare(wynik, wynikt5, allowAll = TRUE)

# Porowanie czasow wykonania - zakomentuj te czesc przed wyslaniem
# porownanie5 <- microbenchmark(
#   sqldf = sql_5(Posts, Comments, Users),
#   base = base_5(Posts, Comments, Users),
#   dplyr = dplyr_5(Posts, Comments, Users),
#   data.table = table_5(Posts, Comments, Users), times = 25)
# Wynik:
# Unit: milliseconds
# expr       min        lq      mean    median        uq       max neval
# sqldf 1424.6788 1463.9216 1508.6768 1503.8714 1534.8745 1718.6981    25
# base  869.4905  897.7540  936.2189  920.9560  963.0554 1091.0194    25
# dplyr  189.5465  198.1553  234.5584  230.6390  250.7826  409.5130    25
# data.table   56.5608   76.8219  105.3434  106.6809  109.9089  270.4078    25

