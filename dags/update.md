CREATE TABLE `resumes_path` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `path` varchar(300) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

ALTER TABLE `resumes_path` ADD INDEX index_path ( `path` ) ;
CREATE TABLE `res` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `path` varchar(300) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

ALTER TABLE `resumes_path` ADD INDEX index_path ( `path` ) ;

load data local infile "/home/ubuntu/airflow/dags/file_path/filenames" into table resumes_path(path);
