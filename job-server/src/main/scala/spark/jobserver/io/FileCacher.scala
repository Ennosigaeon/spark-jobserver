package spark.jobserver.io

import java.io.{BufferedOutputStream, IOException}
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import spark.jobserver.util.Utils

import java.net.URLEncoder
import java.nio.file.{Files, Path}

trait FileCacher {

  val rootDir: Path

  private val logger = LoggerFactory.getLogger(getClass)


  // date format
  val Pattern = "\\d{8}_\\d{6}_\\d{3}".r

  protected def createBinaryName(appName: String, binaryType: BinaryType, uploadTime: DateTime): String = {
    s"${escapeAppName(appName)}-${uploadTime.toString("yyyyMMdd_HHmmss_SSS")}.${binaryType.extension}"
  }

  private def escapeAppName(appName: String) = URLEncoder.encode(appName, "UTF-8")

  // Cache the binary file into local file system.
  protected def cacheBinary(appName: String,
                            binaryType: BinaryType,
                            uploadTime: DateTime,
                            binBytes: Array[Byte]): Path = {
    Utils.createDirectory(rootDir)

    val targetFullBinaryName = createBinaryName(appName, binaryType, uploadTime)
    val tempOutFile = Files.createTempFile(rootDir, targetFullBinaryName + "-", ".tmp")
    val bos = new BufferedOutputStream(Files.newOutputStream(tempOutFile))

    try {
      logger.debug("Writing {} bytes to a temporary file {}", binBytes.length, tempOutFile)
      bos.write(binBytes)
      bos.flush()
    } finally {
      bos.close()
    }

    logger.debug("Renaming the temporary file {} to the target full binary name {}",
      tempOutFile, targetFullBinaryName: Any)

    val renamedFile = tempOutFile.resolveSibling(targetFullBinaryName)
    try {
      Files.move(tempOutFile, renamedFile)
      renamedFile.toFile.deleteOnExit()
    }
    catch {
      case _: IOException =>
        logger.debug("Renaming the temporary file {} failed, another process has probably already updated " +
          "the target file - deleting the redundant temp file", tempOutFile)
        try {
          Files.deleteIfExists(tempOutFile)
        }
        catch {
          case _: Throwable => logger.warn("Could not delete the temporary file {}", tempOutFile)
        }
    }
    renamedFile
  }

  protected def cleanCacheBinaries(appName: String): Unit = {
    Utils.createDirectory(rootDir)

    Files.list(rootDir)
      .filter(p => {
        val prefix = escapeAppName(appName) + "-"
        val name = p.getFileName.toString
        if (name.startsWith(prefix)) {
          val suffix = name.substring(prefix.length)
          (Pattern findFirstIn suffix).isDefined
        } else {
          false
        }
      })
      .forEach(p => Files.deleteIfExists(p))
  }
}
