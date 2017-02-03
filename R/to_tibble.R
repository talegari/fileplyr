#' @title to_tibble
#'
#' @description convert list output of fileply to an object of class tibble
#'
#' @param object list output of fileply
#' @param valuename name of the value list-column
#'
#'
#' @return an object of class tibble
#'
#' @examples
#' # split-apply-combine
#' write.table(mtcars, "mtcars.csv", row.names = FALSE, sep = ",")
#' temp <- fileply(file     = "mtcars.csv"
#'              , groupby = c("carb", "gear")
#'              , fun     = identity
#'              , collect = "list"
#'              , sep     =  ","
#'              , header  = TRUE
#'              )
#' temp
#' to_tibble(temp)
#' unlink("mtcars.csv")
#'
#'
#' @export
#'

to_tibble <- function(object, valuename = "value"){

  firstKey <- object[[1]][[1]]

  # output of split-apply-combine ----
  if(class(firstKey) == "character"){
    if(grepl("|", firstKey)){ # there are at least two group by variables

      # get the column names
      cols        <- vapply(strsplit(strsplit(firstKey, "\\|")[[1]], "=")
                            , function(x){x[[1]]}
                            , character(1)
                            )
      # ensure value name does not clash with 'cols'
      if(valuename %in% cols){
        stop(paste0("There exists a variable named '"
                    , valuename
                    , "'. Choose a different name for value column.")
             )
      }

      names_split <- strsplit(vapply(object, function(x){x[[1]]}
                                     , character(1)
                                     )
                              , "\\|"
                              )
      # function to recover a column after removing 'colname='
      get_acol    <- function(col_number){
        col       <- vapply(names_split
                            , function(x){x[[col_number]]}
                            , character(1)
                            )
        col_clean <- gsub(paste0(cols[col_number], "="), "", col)
        return(col_clean)
      }

      # writing to a tibble
      df          <- tibble::as_tibble(c(lapply(1:length(cols), get_acol)
                                         , list(lapply(object, `[[`, 2))
                                         )
                                       , validate = FALSE
                                       )

      names(df) <- c(cols, valuename)

    } else { # case where there is one groupby variable

      colname   <- strsplit(firstKey, "=")[[1]][[1]]

      # ensure value name does not clash with colname
      if(valuename == colname){
        stop(paste0("There exists a variable named '"
                    , valuename
                    , "'. Choose a different name for value column.")
             )}

    df        <- tibble::tibble(gsub("colname="
                                   , ""
                                   , vapply(object
                                            , function(x){x[[1]]}
                                            , character(1)
                                            )
                                   )
                              , lapply(object
                                       , `[[`
                                       , 2
                                       )
                              )
    names(df) <- c(colname, valuename)
    }
  }

  # output of chunk processing    ----
  if(class(firstKey) %in% c("numeric", "integer")){
    colname   <- "chunk"

      # ensure value name does not clash with colname
      if(valuename == colname){
        stop(paste0("There exists a variable named '"
                    , valuename
                    , "'. Choose a different name for value column.")
             )
      }

    df        <- tibble::tibble(vapply(object
                                       , function(x){x[[1]]}
                                       , numeric(1)
                                       )
                              , lapply(object
                                       , `[[`
                                       , 2
                                       )
                              )
    names(df) <- c(colname, valuename)
  }


  return(df)
}


