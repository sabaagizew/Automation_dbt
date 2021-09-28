CREATE TABLE IF NOT EXISTS "flow" (
  "ID" INT NOT NULL,
  "flow_99" INT NOT NULL,
  "flow_max" INT NOT NULL,
  "flow_median" INT NOT NULL,
  "flow_total" INT NOT NULL,
  "n_obs" INT NOT NULL,
  PRIMARY KEY ("flowid")

) "ENGINE" = InnoDB;

CREATE TABLE IF NOT EXISTS "time" (
  "date" DATETIME NOT NULL,
  "dayofweek" INT NOT NULL,
  "hour" INT NOT NULL,
  "minute" INT NOT NULL,
  "seconds" INT NOT NULL,
  PRIMARY KEY ("date")

) ENGINE=InnoDB;



CREATE TABLE IF NOT EXISTS "flow" (

  "flowid" INT NOT NULL AUTO_INCREMENT,
  "date" DATETIME NOT NULL,
  "flow1" INT NOT NULL,
  "flow2" INT NOT NULL,
  "flow3" INT NOT NULL,
  "flow3" INT NOT NULL,
  "flow3" INT NOT NULL,
  "flowtotal" INT NOT NULL,
  PRIMARY KEY ("flowid") 
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS "mph" (

  "mphid" INT NOT NULL AUTO_INCREMENT,
  "date" DATETIME NOT NULL,
  "mph1" INT NOT NULL,
  "mph2" INT NOT NULL,
  "mph3" INT NOT NULL,
  "mph4" INT NOT NULL,
  "mph5" INT NOT NULL,
  PRIMARY KEY ("mphid") 
) ENGINE=InnoDB;


CREATE TABLE IF NOT EXISTS "ocuppancy" (
  "ocuppancyid" INT NOT NULL AUTO_INCREMENT,
  "date" DATETIME NOT NULL,
  "ocuppancy1" INT NOT NULL,
  "ocuppancy2" INT NOT NULL,
  "ocuppancy3" INT NOT NULL,
  "ocuppancy4" INT NOT NULL,
  "ocuppancy5" INT NOT NULL,
  PRIMARY KEY ("ocuppancyid")
)ENGINE=InnoDB;