package com.guizmaii.udemy.kafka.connect.github.data

import java.time.Instant

import com.guizmaii.udemy.kafka.connect.github.data.utils.SchemaBuilderOps
import org.apache.kafka.connect.data.{Schema, SchemaBuilder}

final case class Account(
  login: String,
  id: String,
  node_id: String,
  avatar_url: String,
  gravatar_id: String,
  url: String,
  html_url: String,
  followers_url: String,
  following_url: String,
  gists_url: String,
  starred_url: String,
  subscriptions_url: String,
  organizations_url: String,
  repos_url: String,
  events_url: String,
  received_events_url: String,
  `type`: String,
  site_admin: Boolean
)

object Account {

  import Schema._
  import SchemaBuilderOps._

  private final val LOGIN               = "login"               -> STRING_SCHEMA
  private final val ID                  = "id"                  -> STRING_SCHEMA
  private final val NODE_ID             = "node_id"             -> STRING_SCHEMA
  private final val AVATAR_URL          = "avatar_url"          -> STRING_SCHEMA
  private final val GRAVATAR_URL        = "gravatar_id"         -> STRING_SCHEMA
  private final val URL                 = "url"                 -> STRING_SCHEMA
  private final val HTML_URL            = "html_url"            -> STRING_SCHEMA
  private final val FOLLOWERS_URL       = "followers_url"       -> STRING_SCHEMA
  private final val FOLLOWING_URL       = "following_url"       -> STRING_SCHEMA
  private final val GISTS_URL           = "gists_url"           -> STRING_SCHEMA
  private final val STARRED_URL         = "starred_url"         -> STRING_SCHEMA
  private final val SUBSCRIPTIONS_URL   = "subscriptions_url"   -> STRING_SCHEMA
  private final val ORGANIZATIONS_URL   = "organizations_url"   -> STRING_SCHEMA
  private final val REPOS_URL           = "repos_url"           -> STRING_SCHEMA
  private final val EVENTS_URL          = "events_url"          -> STRING_SCHEMA
  private final val RECEIVED_EVENTS_URL = "received_events_url" -> STRING_SCHEMA
  private final val TYPE                = "type"                -> STRING_SCHEMA
  private final val SITE_ADMIN          = "site_admin"          -> BOOLEAN_SCHEMA

  final val schema: SchemaBuilder =
    SchemaBuilderS(1, "com.guizmaii.udemy.kafka.connect.github.data.Account")
      .field(LOGIN)
      .field(ID)
      .field(NODE_ID)
      .field(AVATAR_URL)
      .field(GRAVATAR_URL)
      .field(URL)
      .field(HTML_URL)
      .field(FOLLOWERS_URL)
      .field(FOLLOWING_URL)
      .field(GISTS_URL)
      .field(STARRED_URL)
      .field(SUBSCRIPTIONS_URL)
      .field(ORGANIZATIONS_URL)
      .field(REPOS_URL)
      .field(EVENTS_URL)
      .field(RECEIVED_EVENTS_URL)
      .field(TYPE)
      .field(SITE_ADMIN)
      .build

}

final case class Permissions(
  administration: String,
  checks: String,
  contents: String,
  metadata: String,
  pull_requests: String
)

object Permissions {

  import Schema._
  import SchemaBuilderOps._

  private final val ADMINISTRATION = "administration" -> STRING_SCHEMA
  private final val CHECKS         = "checks"         -> STRING_SCHEMA
  private final val CONTENTS       = "contents"       -> STRING_SCHEMA
  private final val METADATA       = "metadata"       -> STRING_SCHEMA
  private final val PULL_REQUESTS  = "pull_requests"  -> STRING_SCHEMA

  final val schema: SchemaBuilder =
    SchemaBuilderS(1, "com.guizmaii.udemy.kafka.connect.github.data.Permissions")
      .field(ADMINISTRATION)
      .field(CHECKS)
      .field(CONTENTS)
      .field(METADATA)
      .field(PULL_REQUESTS)
      .build
}

final case class Installation(
  id: String,
  account: Account,
  repository_selection: String,
  access_tokens_url: String,
  repositories_url: String,
  html_url: String,
  app_id: String,
  target_id: String,
  target_type: String,
  permissions: Permissions,
  events: List[String], // TODO: Is it really containing String ???
  created_at: Instant,
  updated_at: Instant,
  single_file_name: String
)

object Installation {

  import Schema._
  import SchemaBuilderOps._

  private final val ID                   = "id"                   -> STRING_SCHEMA
  private final val ACCOUNT              = "account"              -> Account.schema
  private final val REPOSITORY_SELECTION = "repository_selection" -> STRING_SCHEMA
  private final val ACCESS_TOKENS_URL    = "access_tokens_url"    -> STRING_SCHEMA
  private final val REPOSITORIES_URL     = "repositories_url"     -> STRING_SCHEMA
  private final val HTML_URL             = "html_url"             -> STRING_SCHEMA
  private final val APP_ID               = "app_id"               -> STRING_SCHEMA
  private final val TARGET_ID            = "target_id"            -> STRING_SCHEMA
  private final val TARGET_TYPE          = "target_type"          -> STRING_SCHEMA
  private final val PERMISSIONS          = "permissions"          -> Permissions.schema
  private final val EVENTS               = "events"               -> ARRAY_STRING_SCHEMA // TODO: Is it really containing String ???
  private final val CREATED_AT           = "created_at"           -> STRING_SCHEMA
  private final val UPDATED_AT           = "updated_at"           -> STRING_SCHEMA
  private final val SINGLE_FILE_NAME     = "single_file_name"     -> STRING_SCHEMA

  final val schema: SchemaBuilder =
    SchemaBuilderS(1, "com.guizmaii.udemy.kafka.connect.github.data.Installation")
      .field(ID)
      .field(ACCOUNT)
      .field(REPOSITORY_SELECTION)
      .field(ACCESS_TOKENS_URL)
      .field(REPOSITORIES_URL)
      .field(HTML_URL)
      .field(APP_ID)
      .field(TARGET_ID)
      .field(TARGET_TYPE)
      .field(PERMISSIONS)
      .field(EVENTS)
      .field(CREATED_AT)
      .field(UPDATED_AT)
      .field(SINGLE_FILE_NAME)
      .build

}

final case class Repositories(
  id: String,
  node_id: String,
  name: String,
  full_name: String,
  `private`: Boolean
)

object Repositories {

  import Schema._
  import SchemaBuilderOps._

  private final val ID        = "id"        -> STRING_SCHEMA
  private final val NODE_ID   = "node_id"   -> STRING_SCHEMA
  private final val NAME      = "name"      -> STRING_SCHEMA
  private final val FULL_NAME = "full_name" -> STRING_SCHEMA
  private final val PRIVATE   = "private"   -> BOOLEAN_SCHEMA

  final val schema: SchemaBuilder =
    SchemaBuilderS(1, "com.guizmaii.udemy.kafka.connect.github.data.Repositories")
      .field(ID)
      .field(NODE_ID)
      .field(NAME)
      .field(FULL_NAME)
      .field(PRIVATE)
      .build
}

final case class AppInstallationEvent(
  action: String,
  installation: Installation,
  repositories: List[Repositories],
  requester: String,
  sender: Account
)

object AppInstallationEvent {

  import Schema._
  import SchemaBuilderOps._

  private final val ACTION       = "action"       -> STRING_SCHEMA
  private final val INSTALLATION = "installation" -> Installation.schema
  private final val REPOSITORIES = "repositories" -> SchemaBuilder.array(Repositories.schema)
  private final val REQUESTER    = "requester"    -> STRING_SCHEMA
  private final val SENDER       = "sender"       -> Account.schema

  final val schema: SchemaBuilder =
    SchemaBuilderS(1, "com.guizmaii.udemy.kafka.connect.github.data.AppInstallationEvent")
      .field(ACTION)
      .field(INSTALLATION)
      .field(REPOSITORIES)
      .field(REQUESTER)
      .field(SENDER)
      .build
}
