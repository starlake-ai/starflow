package ai.starlake.impact

import ai.starlake.config.Settings
import ai.starlake.utils.{GitSnapshot, ProjectCheckout}
import better.files.File

import scala.util.{Failure, Success, Try}

/** One side of a comparison: a project directory, its label in the report, and the temporary
  * checkout to delete afterwards when the side comes from git.
  */
final case class ImpactSide(dir: File, label: String, checkout: Option[ProjectCheckout]) {
  def cleanup(): Unit = checkout.foreach(_.cleanup())
}

/** Turns the `--base` / `--head` / `--path1` / `--path2` options into two project directories. */
object ImpactSides {

  private def repoOf(projectDir: File): Try[File] =
    GitSnapshot.topLevel(projectDir).recoverWith { case _ =>
      Failure(
        new IllegalArgumentException(
          s"$projectDir is not inside a git repository, which --base and --head need; use --path1 / --path2 to compare directories"
        )
      )
    }

  /** `git rev-parse --verify --quiet` fails without a message on an unknown ref. */
  private def commitOf(repo: File, ref: String): Try[String] =
    GitSnapshot.resolveCommit(repo, ref).recoverWith { case _ =>
      Failure(new IllegalArgumentException(s"unknown git ref '$ref'"))
    }

  private def isProject(dir: File): Boolean = (dir / "metadata").isDirectory

  /** A side given as a directory must be a project: a wrong path would otherwise load as an empty
    * project, and every node would show as added (or nothing would change).
    */
  private def requireProject(dir: File, shown: String): Try[File] =
    if (isProject(dir)) Success(dir)
    else {
      val hint =
        if (dir.name == "metadata") ", pass the directory that contains the metadata folder"
        else ""
      Failure(
        new IllegalArgumentException(
          s"$shown is not a project directory: it has no metadata folder$hint"
        )
      )
    }

  /** Resolves the old side first, then the new one. A checkout already extracted is deleted when
    * the other side fails. Only the old side of a git comparison may lack a project, when the
    * merge-base predates it: everything then shows as added, and its label says so.
    */
  def resolve(config: ImpactConfig)(implicit settings: Settings): Try[(ImpactSide, ImpactSide)] = {
    val projectDir = File(settings.appConfig.root)
    val workingTree = config.path2.isEmpty && config.head.isEmpty

    def oldSide: Try[ImpactSide] = (config.path1, config.base) match {
      case (Some(p), _) => requireProject(File(p), p).map(ImpactSide(_, p, None))
      case (None, Some(b)) =>
        for {
          repo <- repoOf(projectDir)
          base <- commitOf(repo, b)
          head <- commitOf(repo, config.head.getOrElse("HEAD"))
          mb   <- GitSnapshot.mergeBase(repo, base, head)
          co   <- GitSnapshot.projectAt(projectDir, mb)
        } yield {
          val absent = if (isProject(co.projectDir)) "" else ", no project at this commit"
          ImpactSide(co.projectDir, s"merge-base of $b (${mb.take(7)}$absent)", Some(co))
        }
      case (None, None) =>
        Failure(new IllegalArgumentException("one of --base or --path1 is required"))
    }

    def newSide: Try[ImpactSide] = (config.path2, config.head) match {
      case (Some(p), _) => requireProject(File(p), p).map(ImpactSide(_, p, None))
      case (None, Some(h)) =>
        for {
          repo   <- repoOf(projectDir)
          commit <- commitOf(repo, h)
          co     <- GitSnapshot.projectAt(projectDir, commit)
          side <- requireProject(co.projectDir, s"$projectDir at $h (${commit.take(7)})")
            .map(dir => ImpactSide(dir, s"$h (${commit.take(7)})", Some(co)))
            .recoverWith { case e => co.cleanup(); Failure(e) }
        } yield side
      case (None, None) =>
        requireProject(projectDir, projectDir.pathAsString)
          .map(ImpactSide(_, "working tree", None))
    }

    for {
      // checked first, so that no checkout is extracted for nothing
      _ <- if (workingTree) requireProject(projectDir, projectDir.pathAsString) else Success(())
      o <- oldSide
      n <- newSide.recoverWith { case e => o.cleanup(); Failure(e) }
    } yield (o, n)
  }

  /** Runs `f` on the two sides, then deletes their checkouts whatever happens. */
  def withSides[A](config: ImpactConfig)(f: (ImpactSide, ImpactSide) => Try[A])(implicit
    settings: Settings
  ): Try[A] =
    resolve(config).flatMap { case (oldSide, newSide) =>
      try Try(f(oldSide, newSide)).flatten
      finally {
        oldSide.cleanup()
        newSide.cleanup()
      }
    }
}
