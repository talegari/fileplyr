#' @title fileplyr
#'
#' @description A package to perform chunk processing or split-apply-combine
#'   on data in a delimited file(example: CSV) across multiple cores of a single
#'   machine with low memory footprint. These functions are a convenient
#'   wrapper over the versatile package \pkg{datadr}.
#'
#'
#'   \strong{Memory vs time tradeoff}: Handling data that cannot be loaded into
#'   RAM involves a tradeoff with memory usage versus time required to complete
#'   the computation task. fileplyr lies at one end of the spectrum where it is
#'   assumed that the user has time at disposal as opposed to hardware facility.
#'   The size of the file to be processed has to be large enough to observe the
#'   advantage of the package.
#'
#'   \strong{Audience}: Analysts with laptop/desktop with multiple cores and
#'   limited RAM(and lots of time) might find the package helpful for
#'   prototyping tasks. A proper estimation of memory usage will let other
#'   applications to function comfortably along with a R process running a
#'   fileplyr job.
#'
#' @import assertthat
#' @import parallel
#' @importFrom tibble as_tibble
#' @importFrom utils capture.output
#' @import datadr
#'
"_PACKAGE"
