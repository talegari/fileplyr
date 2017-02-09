#' @title ddfply
#'   
#' @description performs chunk processing or split-apply-combine on the data
#'   in a distributed data frame(ddf)
#'   
#' @details  see \code{\link{fileply}}
#'   
#' @param ddfdir (string) path of ddf directory
#' @param groupby (character vector) Columns names to used to split the data(if 
#'   missing, \code{fun} is applied on each chunk)
#' @param fun (object of class \emph{function}) function to apply on each subset
#'   after the split
#' @param collect (string) Collect the result as \code{list} or \code{dataframe}
#'   or \code{none}. \code{none} keeps the resulting ddo on disk.
#' @param nbins (positive integer) Number of directories into which the 
#'   distributed dataframe (ddf) or distributed data object (ddo) is distributed
#' @param chunk (positive integer) Number of rows of the file to be read at a 
#'   time
#' @param spill (positive integer) Maximum number of rows of any subset 
#'   resulting from split
#' @param cores (positive integer) Number of cores to be used in parallel
#' @param buffer (positive integer) Size of batches of key-value pairs to be 
#'   passed to the map OR Size of the batches of key-value pairs to flush to 
#'   intermediate storage from the map output OR Size of the batches of 
#'   key-value pairs to send to the reduce
#' @param temploc (string) Path where intermediary files are kept
#' @param ... Arguments to be passed to \code{data.table} function asis.
#'   
#' @return list or a dataframe or a TRUE(when collect is 'none').
#'   
#' @examples
#' write.table(mtcars, "mtcars.csv", row.names = FALSE, sep = ",")
#' # create a ddf by keeping `keepddf = TRUE`
#' co <- capture.output(temp <- fileply("mtcars.csv"
#'                                      , groupby = c("carb", "gear")
#'                                      , fun     = identity
#'                                      , collect = "list"
#'                                      , sep     =  ","
#'                                      , header  = TRUE
#'                                      , keepddf = TRUE)
#'                      , file = NULL
#'                      , type = "message"
#'                      )
#' # use the ddf instead of reading the CSV again
#' temp2 <- ddfply(file.path(strsplit(co[6], ": ")[[1]][2], "data")
#'                 , groupby = c("gear")
#'                 , fun     = identity
#'                 , collect = "list"
#'                 , sep     =  ","
#'                 , header  = TRUE
#'                 )
#' temp2
#' unlink("mtcars.csv")
#' unlink(strsplit(co[6], ": ")[[1]][2], recursive = TRUE)
#' 
#' @export
#' 
ddfply  <- function(ddfdir
                    , groupby
                    , fun          = identity
                    , collect      = "none"
                    , temploc      = getwd()
                    , nbins        = 10
                    , chunk        = 5e4
                    , spill        = 1e6
                    , cores        = 1
                    , buffer       = 1e9
                    , ...){
  
  # assertions                                           ----
  assert_that(is.dir(ddfdir))
  assert_that(is.readable(ddfdir))
  job <- "cp"
  if(!missing(groupby)){
    job <- "sac"  
    assert_that(is.character(groupby))
  }
  assert_that(is.function(fun))
  assert_that(collect %in% c("list", "dataframe", "none"))
  assert_that(is.writeable(temploc))
  assert_that(is.count(nbins))
  assert_that(is.count(chunk))
  assert_that(is.count(spill))
  assert_that(is.count(cores))
  assert_that(is.count(buffer))
  
  # set up parallel backend                              ----
  
  cl <- makeCluster(cores)
  control <- localDiskControl(cluster                    = cl
                              , map_buff_size_bytes      = buffer
                              , reduce_buff_size_bytes   = buffer
                              , map_temp_buff_size_bytes = buffer
  )
  
  on.exit(stopCluster(cl), add = TRUE)
  
  # creating data connection                             ----
  timecode    <- gsub(":", "_", gsub(" ", "_", Sys.time()))
  ranLetters  <- paste0(do.call(paste0, as.list(sample(letters, 10)))
                        , "__"
                        , timecode
  )
  message("----")
  message("Job ID: ", ranLetters)
  ply_dir    <- file.path(temploc, paste0("ply_", ranLetters))
  dir.create(ply_dir)
  
  # ensure removals at the end
  if(collect != "none"){
    on.exit(unlink(file.path(ply_dir), recursive = TRUE)
            , add = TRUE)
  }
  
  fileDiskConn <- localDiskConn(ddfdir, verbose  = FALSE)
  
  # create a ddf connection                              ----
  
  suppressMessages(
    fileddf <- ddf(conn = fileDiskConn, control = control))
  
  # split using `groupby` variable(s) and apply function ----
  if(job == "sac"){
    splitapply_code <-
      "divided <-
    divide(data      = fileddf
    , by      = groupby
    , spill   = spill
    , output  = localDiskConn(file.path(ply_dir, 'divided')
    , autoYes = TRUE
    , nBins   = nbins
    , verbose = TRUE
    )
    , control     = control
    , postTransFn = fun
    )"
  }
  
  if(job == "cp"){
    splitapply_code <-
      "divided <-
    drLapply(X        = fileddf
    , FUN    = fun
    , output = localDiskConn(file.path(ply_dir, 'divided')
    , autoYes = TRUE
    , nBins   = nbins
    , verbose = TRUE
    )
    , control     = control
    )"
  }
  
  
  message("Aggregating  ... ", appendLF = FALSE)
  start_splitapply <- Sys.time()
  
  suppressMessages(
    capture.output(
      eval(parse(text = splitapply_code))
      , file = tempfile()
    )
  )
  
  end_splitapply  <- Sys.time()
  splitapply_time <- end_splitapply - start_splitapply
  
  message("Completed in "
          , round(splitapply_time, 1)
          , " "
          , attributes(splitapply_time)$units
  )
  
  # collect the results                                  ----
  
  result <- NULL
  if(collect == "list"){
    collectCode   <- "result <- recombine(divided
    , combine = combCollect
    , control = control
    )"
  }
  
  if(collect == "dataframe"){
    collectCode   <- "result <- recombine(divided
    , combine = combRbind
    , control = control
    )"
  }
  
  if(collect != "none"){
    message("Collecting   ... ", appendLF = FALSE)
    start_collect <- Sys.time()
    suppressWarnings(
      suppressMessages(
        capture.output(
          eval(parse(text = collectCode))
          , file = tempfile()
        )
      ))
    
    end_collect  <- Sys.time()
    collect_time <- end_collect - start_collect
    
    message("Completed in "
            , round(collect_time, 1)
            , " "
            , attributes(collect_time)$units
    )
  }
  
  if(collect == "none"){
    message("Output Directory: ", ply_dir)
  }
  message("Done!")
  message("----")
  if(collect == "none"){
    return(invisible(TRUE))
  } else {
    return(result)
  }
}
