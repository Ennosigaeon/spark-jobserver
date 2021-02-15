package spark.jobserver.io

import org.joda.time.DateTime
import org.scalatest.{FunSpecLike, Matchers}
import spark.jobserver.util.Utils

import java.nio.file.{Files, Path, Paths}

class FileCacherRandomDirSpec extends FileCacher with FunSpecLike with Matchers {

  private val parent = Paths.get("/tmp/spark-jobserver/")
  Utils.createDirectory(parent)
  override val rootDir: Path = Files.createTempDirectory(parent, "")

  it("should create cache directory if it doesn't exist") {
    val bytes = "some test content".toCharArray.map(_.toByte)
    val appName = "test-file-cached"
    val currentTime = DateTime.now
    val targetBinName = createBinaryName(appName, BinaryType.Jar, currentTime)
    cacheBinary(appName, BinaryType.Jar, currentTime, bytes)
    val file = rootDir.resolve(targetBinName)
    Files.exists(file) should be(true)
    cleanCacheBinaries(appName)
    Files.exists(file) should be(false)
  }
}
