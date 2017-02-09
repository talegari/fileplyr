#' @title fileply
#'   
#' @description performs chunk processing or split-apply-combine on the data
#'   in a delimited file (example: CSV).
#'   
#' @details \itemize{\item \strong{Reading Stage:} The delimited file (example:
#'   CSV) is read in smaller chunks to create a distributed dataframe or 
#'   \strong{ddf} on disk. The number of lines read at once is specified by 
#'   \code{chunk} argument and \code{nbins} specify the number of 
#'   sub-directories the data is distributed. The \code{...} are additional 
#'   inputs to \code{data.table} function for reading the delimited file. \item 
#'   \strong{Split and Apply Stage:} The variables in \code{groupby} are used to
#'   split the data and load only the subset(possibly many if multiple cores are
#'   in action) into the memory. If \code{groupby}  is missing, chunkwise 
#'   processing is performed on each subset of the distributed dataframe. A user
#'   defined \code{fun} is applied and results are written to a distributed 
#'   object(list or a KV pairs) on disk. \item \strong{Combine Stage:} The 
#'   distributed data object(\strong{ddo}) is read into memory depending on 
#'   \code{collect} argument. The default is set to 'none' which would not the 
#'   data back into memory.}
#'   
#'   \strong{Memory usage: } While processing heavy files(many times the RAM 
#'   size), each core might hold a maximum of 800 MB to 1GB of memory overhead 
#'   without accounting for the memory used by the user defind function. Memory 
#'   usage depending on size of the subset, how many times it is copied by the 
#'   user function, how frequently is \code{gc} called. Using appropriate number
#'   of cores keeps memory utilization in check. Setting a smaller \code{buffer}
#'   value keeps memory usage low, see \code{\link[datadr]{localDiskControl}}, 
#'   but makes the execution slower.
#'   
#' @param file (string) path to input delimited file
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
#' @param keepddf (flag) whether to save the distributed dataframe (on the disk)
#' @param temploc (string) Path where intermediary files are kept
#' @param ... Arguments to be passed to \code{data.table} function asis.
#'   
#' @return list or a dataframe or a TRUE(when collect is 'none').
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
#' unlink("mtcars.csv")
#' 
#' # chunkwise processing
#' write.table(mtcars, "mtcars.csv", row.names = FALSE, sep = ",")
#' temp <- fileply(file     = "mtcars.csv"
#'              , chunk   = 10
#'              , fun     = function(x){list(nrow(x))}
#'              , collect = "dataframe"
#'              , sep     =  ","
#'              , header  = TRUE
#'              )
#' temp
#' unlink("mtcars.csv")
#'
#' # example for collect='none'
#' write.table(mtcars, "mtcars.csv", row.names = FALSE, sep = ",")
#' outdir <- utils::capture.output(temp <- fileply(file      = "mtcars.csv"
#'                                                 , groupby = c("carb", "gear")
#'                                                 , fun     = identity
#'                                                 , sep     =  ","
#'                                                 , header  = TRUE
#'                                                 )
#'                                 , file = NULL
#'                                 , type = "message"
#'                                 )
#' outdir <- gsub("Output Directory: ", "", outdir[5])
#' diskKV <- datadr::ddo(datadr::localDiskConn(outdir))
#' diskKV
#' diskKV[[1]]
#' unlink(outdir, recursive = TRUE)
#' unlink("mtcars.csv")
#' 
#' @export
#' 
fileply  <- function(file
                 , groupby
                 , fun          = identity
                 , collect      = "none"
                 , temploc      = getwd()
                 , nbins        = 10
                 , chunk        = 5e4
                 , spill        = 1e6
                 , cores        = 1
                 , buffer       = 1e9
                 , keepddf      = FALSE
                 , ...){

  # assertions                                           ----
  assert_that(is.readable(file))
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
  assert_that(is.flag(keepddf))


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
  if(!keepddf){
    on.exit(unlink(file.path(ply_dir, 'data'), recursive = TRUE)
            , add = TRUE)
  }
  if(collect != "none"){
    on.exit(unlink(file.path(ply_dir, "divided"), recursive = TRUE)
            , add = TRUE)
  }
  if(!keepddf && collect != "none"){
    on.exit(unlink(file.path(ply_dir), recursive = TRUE)
            , add = TRUE)
  }

  fileDiskConn <- localDiskConn(file.path(ply_dir, "data")
                                , autoYes  = TRUE
                                , nBins    = nbins
                                , verbose  = FALSE
                                )

  # read the data                                        ----
  readCode <- "drRead.table(file           = file
                            , output       = fileDiskConn
                            , rowsPerBlock = chunk
                            , control      = control
                            , ...
                            )"

  message("Reading data ... ", appendLF = FALSE)
  start_read <- Sys.time()

  suppressMessages(
      capture.output(
        eval(parse(text = readCode))
        , file = tempfile()
      ))

  end_read  <- Sys.time()
  read_time <- end_read - start_read

  message("Completed in "
          , round(read_time, 1)
          , " "
          , attributes(read_time)$units
          )

  # create a ddf connection                              ----
  # added control here during rehash

  suppressMessages(
    fileddf <- ddf(conn = fileDiskConn
                   , control = control
                   )
    )

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

  if(collect == "none" || keepddf){
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


