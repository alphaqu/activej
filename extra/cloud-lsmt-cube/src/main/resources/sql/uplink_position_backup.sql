CREATE TABLE IF NOT EXISTS `{position}`
(
    `backup_revision` BIGINT       NOT NULL,
    `revision_id`     BIGINT       NOT NULL,
    `partition_id`    VARCHAR(255) NOT NULL,
    `filename`        VARCHAR(255) NOT NULL,
    `remainder`       INT          NOT NULL,
    `position`        BIGINT       NOT NULL
) ENGINE = InnoDB
    DEFAULT CHARSET = utf8;
