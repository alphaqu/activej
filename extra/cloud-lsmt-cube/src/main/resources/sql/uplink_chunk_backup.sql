CREATE TABLE IF NOT EXISTS `{chunk}`
(
    `backup_revision`  BIGINT       NOT NULL,
    `id`               BIGINT       NOT NULL,
    `aggregation`      VARCHAR(255) NOT NULL,
    `measures`         TEXT         NOT NULL,
    `min_key`          TEXT         NOT NULL,
    `max_key`          TEXT         NOT NULL,
    `item_count`       INT          NOT NULL,
    `added_revision`   BIGINT       NOT NULL,
    `removed_revision` BIGINT       NULL,
    PRIMARY KEY (`backup_revision`, `id`)
) ENGINE = InnoDB
   DEFAULT CHARSET = utf8;
