CREATE TABLE IF NOT EXISTS `{revision}`
(
    `revision` BIGINT NOT NULL,
    PRIMARY KEY (`revision`)
) ENGINE = InnoDB;

INSERT INTO {revision} (`revision`)
SELECT 0
WHERE NOT EXISTS (SELECT * FROM {revision});
