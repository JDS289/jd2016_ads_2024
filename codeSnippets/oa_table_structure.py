# census_path = fynesse.access.download_csv("https://raw.githubusercontent.com/JDS289/jd2016_ads_2024/refs/heads/main/census2021-ts062-oa.csv")

"""%%sql
USE `ads_2024`;
--
-- Table structure for table `census2021_ts062_oa`
--
CREATE TABLE IF NOT EXISTS `census2021_ts062_oa` (
  `geography` varchar(9) COLLATE utf8_bin NOT NULL,
  `total`     int(6) unsigned NOT NULL,
  `l123`      int(6) unsigned NOT NULL,
  `l456`      int(6) unsigned NOT NULL,
  `l7`        int(6) unsigned NOT NULL,
  `l89`       int(6) unsigned NOT NULL,
  `l1011`     int(6) unsigned NOT NULL,
  `l12`       int(6) unsigned NOT NULL,
  `l13`       int(6) unsigned NOT NULL,
  `l14`       int(6) unsigned NOT NULL,
  `l15`       int(6) unsigned NOT NULL,
  PRIMARY KEY (`geography`)
);"""

"""conn.cursor().execute(f"LOAD DATA LOCAL INFILE '{census_path}' INTO TABLE `census2021_ts062_oa` FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED by '\"' LINES STARTING BY '' TERMINATED BY '\n'"
                     + "IGNORE 1 ROWS (@dummy, geography, @dummy, total, l123, l456, l7, l89, l1011, l12, l13, l14, l15);")


conn.commit()"""



"""
%%sql
USE ads_2024;

ALTER TABLE census2021_ts062_oa ADD COLUMN boundary GEOMETRY;
"""
