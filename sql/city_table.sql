CREATE TABLE `city_table` (
  `id` bigint(64) NOT NULL AUTO_INCREMENT COMMENT '城市主键ID',
  `code` varchar(40) NOT NULL COMMENT '城市编号',
  `name` varchar(255) NOT NULL COMMENT '城市名称',
  `tree_path` varchar(150) DEFAULT '' COMMENT '城市路径',
  `remark` varchar(255) DEFAULT NULL COMMENT '城市备注',
  `parent_code` varchar(40) NOT NULL DEFAULT '0' COMMENT '父节点code',
  `level` int(2) NOT NULL COMMENT '城市级别',
  PRIMARY KEY (`id`),
  KEY `index_parent_code` (`parent_code`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='城市信息表'