package ai.starlake.lineage

case class TaskViewDependencyNode(
  data: TaskViewDependency,
  children: List[TaskViewDependencyNode]
) {
  def print(level: Int = 0): Unit = {
    println("  " * level + data.name)
    children.foreach(_.print(level + 1))
  }
  def isTask(): Boolean = data.typ == "task"
}

object TaskViewDependencyNode {

  def dependencies(
    entities: List[TaskViewDependency],
    relations: List[TaskViewDependency]
  ): List[TaskViewDependencyNode] = {
    val result = entities.map { entity =>
      dependencies(entity, entities, relations)
    }
    result
  }

  def dependencies(
    entity: TaskViewDependency,
    entities: List[TaskViewDependency],
    relations: List[TaskViewDependency]
  ): TaskViewDependencyNode = dependencies(entity, entities, relations, Set.empty)

  /** @param ancestors
    *   names already on the path from the root down to `entity`. A parent that is an ancestor
    *   closes a cycle (a task reading its own sink, or two tasks referencing each other) and is
    *   dropped rather than expanded, which would otherwise recurse until the stack overflows.
    */
  private def dependencies(
    entity: TaskViewDependency,
    entities: List[TaskViewDependency],
    relations: List[TaskViewDependency],
    ancestors: Set[String]
  ): TaskViewDependencyNode = {
    val thisEntityRelations =
      relations
        .filter(_.name == entity.name)
        .groupBy(x => x.name + "." + x.parent)
        .view
        .mapValues(_.head)
        .values
        .toList
    val path = ancestors + entity.name.toLowerCase()
    val parentEntities = thisEntityRelations
      .filterNot(r => path.contains(r.parent.toLowerCase()))
      .flatMap(r => entities.find(_.name == r.parent))
    val deps = parentEntities.map { parentEntity =>
      dependencies(parentEntity, entities, relations, path)
    }
    TaskViewDependencyNode(entity, deps)
  }
}
