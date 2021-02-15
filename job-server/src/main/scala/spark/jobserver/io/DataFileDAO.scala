package spark.jobserver.io

import com.typesafe.config._

import java.io._
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import org.apache.commons.io.FileUtils
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import spark.jobserver.util.{DirectoryException, Utils}

import scala.collection.mutable

object DataFileDAO {
  val EXTENSION = ".dat"
  val META_DATA_FILE_NAME = "files.data"
}

case class DataFileInfo(appName: String, uploadTime: DateTime)

class DataFileDAO(config: Config) {
  private val logger = LoggerFactory.getLogger(getClass)

  // set of files managed by this class
  private val files = mutable.HashSet.empty[String]

  val rootDir: Path = {
    val rootDir = Paths.get(config.getString("spark.jobserver.datadao.rootdir")).normalize().toAbsolutePath
    logger.trace("rootDir is {}", rootDir)
    // create the data directory if it doesn't exist
    Utils.createDirectory(rootDir)
    rootDir
  }

  private val dataFile: Path = {
    val dataFile = rootDir.resolve(DataFileDAO.META_DATA_FILE_NAME)
    // read back all files info during startup
    if (Files.exists(dataFile)) {
      val in = new DataInputStream(new BufferedInputStream(Files.newInputStream(dataFile)))
      try {
        while (true) {
          val dataInfo = readFileInfo(in)
          addFile(dataInfo.appName)
        }
      } catch {
        case _: EOFException => // do nothing
      } finally {
        in.close()
      }
    }
    else {
      Files.createFile(dataFile)
    }
    dataFile
  }

  // Don't buffer the stream. I want the apps meta data log directly into the file.
  // Otherwise, server crash will lose the buffer data.
  private val dataOutputStream = new DataOutputStream(
    Files.newOutputStream(dataFile, StandardOpenOption.APPEND))

  def shutdown() {
    try {
      dataOutputStream.close()
    } catch {
      case _: Throwable =>
        logger.error("unable to close output stream")
    }
  }

  /**
    * save the given data into a new file with the given prefix, a time stamp is appended to
    * ensure uniqueness
    */
  def saveFile(aNamePrefix: String, uploadTime: DateTime, aBytes: Array[Byte]): String = {
    // The order is important. Save the file first and then log it into meta data file.
    val fileName = createFileName(aNamePrefix, uploadTime) + DataFileDAO.EXTENSION
    val outFile = verifyRootDir(fileName, requireExistence = false)
    val name = outFile.normalize().toString

    Files.createFile(outFile)
    val bos = new BufferedOutputStream(Files.newOutputStream(outFile))
    try {
      logger.debug("Writing {} bytes to file {}", aBytes.length, name)
      bos.write(aBytes)
      bos.flush()
    } finally {
      bos.close()
    }

    // log it into meta data file
    writeFileInfo(dataOutputStream, DataFileInfo(name, uploadTime))

    // track the new file in memory
    addFile(name)
    name
  }

  private def writeFileInfo(out: DataOutputStream, aInfo: DataFileInfo) {
    out.writeUTF(aInfo.appName)
    out.writeLong(aInfo.uploadTime.getMillis)
  }

  def readFile(aName: String): Array[Byte] = {
    val p = verifyRootDir(aName)
    Files.readAllBytes(p)
  }

  def deleteAll(): Unit = {
    FileUtils.deleteDirectory(rootDir.toFile)
    Files.createDirectory(rootDir)
    files.clear()
  }

  def deleteFile(aName: String): Unit = {
    val p = verifyRootDir(aName)
    if (Files.isDirectory(p)) {
      throw DirectoryException()
    }
    Files.delete(p)
    files -= aName
  }

  private def verifyRootDir(aName: String, requireExistence: Boolean = true): Path = {
    val path = rootDir.resolve(aName)
    if (!path.normalize().toAbsolutePath.startsWith(rootDir) ||
      (requireExistence && !files.contains(path.normalize().toString))) {
      // only allow access to the file if it is known to this class,
      // otherwise this could be abused
      throw new SecurityException(s"$aName not in data root")
    }
    path
  }

  private def readFileInfo(in: DataInputStream) = DataFileInfo(in.readUTF, new DateTime(in.readLong))

  private def addFile(aName: String) {
    files += aName
  }

  def listFiles: Set[String] = files.toSet

  private def createFileName(aName: String, uploadTime: DateTime): String =
    aName + "-" + uploadTime.toString().replace(':', '_')

  private def readError(in: DataInputStream) = {
    val error = in.readUTF()
    if (error == "") None else Some(new Throwable(error))
  }

}
