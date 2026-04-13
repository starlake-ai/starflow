package ai.starlake.schema.model

/** User / Group and Service accounts rights on a subset of the table.
  * @param name
  *   : This Row Level Security unique name
  * @param predicate
  *   : The condition that goes to the WHERE clause and limitt the visible rows.
  * @param grants
  *   : user / groups / service accounts to which this security level is applied. ex :
  *   user:me@mycompany.com,group:group@mycompany.com,serviceAccount:mysa@google-accounts.com,domain:starlake.ai
  */
case class RowLevelSecurity(
  name: String,
  predicate: String = "TRUE",
  grants: Set[String] = Set.empty,
  description: String = ""
) extends SecurityLevel {

  def asMap(): Map[String, String] = Map(
    "rlsName"        -> name,
    "rlsPredicate"   -> predicate,
    "rlsGrants"      -> grants.mkString(","),
    "rlsDescription" -> description
  )
  override def toString: String = {
    s"RowLevelSecurity(name=$name, predicate=$predicate, grants=$grants, description=$description)"
  }

  def this() = this("") // Should never be called. Here for Jackson deserialization only

  def grantees(): Set[(UserType, String)] = {
    grants.flatMap(grant => grant.split(",").map(_.replaceAll("\"", ""))).map { user =>
      val res = user.split(':')
      require(res.length == 2)
      (UserType.fromString(res(0).trim), res(1).trim)
    }
  }

  def compare(other: RowLevelSecurity): ListDiff[Named] =
    AnyRefDiff.diffAnyRef(this.name, this, other)

}

object RowLevelSecurity {

  def parse(input: String): RowLevelSecurity = {
    val components = input.split('/')
    require(components.length >= 3)
    val name = components(0)
    val filter = components(1)
    val users = components.drop(2)
    RowLevelSecurity(name, filter, users.toSet)
  }
}
