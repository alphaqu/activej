CREATE TABLE IF NOT EXISTS `{revision}`
(
    `revision` BIGINT       NOT NULL,
    `added_at` TIMESTAMP    NOT NULL    DEFAULT CURRENT_TIMESTAMP,
    `added_by` VARCHAR(255) NULL,
    PRIMARY KEY (`revision`)
) ENGINE = InnoDB;

INSERT INTO {revision} (`revision`, `added_by`)
SELECT 0, '[ROOT]'
WHERE NOT EXISTS (SELECT * FROM {revision});
