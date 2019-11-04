package com.guizmaii.udemy.kafka.connect.github

import org.apache.kafka.common.config.ConfigDef.{ Importance, Type }
import org.apache.kafka.common.config.{ AbstractConfig, ConfigDef }

object GithubConnectorConfig {

  final val GITHUB_ID     = "github.id"
  final val GITHUB_ID_DOC = "TODO"

  final val GITHUB_CLIENT_ID     = "github.client.id"
  final val GITHUB_CLIENT_ID_DOC = "TODO"

  final val GITHUB_CLIENT_SECRET     = "github.client.secret"
  final val GITHUB_CLIENT_SECRET_DOC = "TODO"

  final val GITHUB_WEBHOOK_SECRET     = "github.webhook.secret"
  final val GITHUB_WEBHOOK_SECRET_DOC = "TODO"

  final val GITHUB_PRIVATE_KEY     = "github.private.key"
  final val GITHUB_PRIVATE_KEY_DOC = "TODO"

  final val config =
    new ConfigDef()
      .define(GITHUB_ID, Type.STRING, Importance.HIGH, GITHUB_CLIENT_ID_DOC)
      .define(GITHUB_CLIENT_ID, Type.STRING, Importance.HIGH, GITHUB_CLIENT_ID_DOC)
      .define(GITHUB_CLIENT_SECRET, Type.STRING, Importance.HIGH, GITHUB_CLIENT_SECRET_DOC)
      .define(GITHUB_WEBHOOK_SECRET, Type.STRING, Importance.HIGH, GITHUB_WEBHOOK_SECRET_DOC)
      .define(GITHUB_PRIVATE_KEY, Type.STRING, Importance.HIGH, GITHUB_PRIVATE_KEY_DOC)
}

final case class GithubConnectorConfig(parsedConfig: java.util.Map[String, String])
    extends AbstractConfig(GithubConnectorConfig.config, parsedConfig)
