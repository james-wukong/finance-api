/*
 Navicat MySQL Data Transfer

 Source Server         : mysql_docker
 Source Server Type    : MySQL
 Source Server Version : 80200 (8.2.0)
 Source Host           : localhost:3306
 Source Schema         : finance_api

 Target Server Type    : MySQL
 Target Server Version : 80200 (8.2.0)
 File Encoding         : 65001

 Date: 19/02/2024 23:40:04
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for company_ticker
-- ----------------------------
DROP TABLE IF EXISTS `company_ticker`;
CREATE TABLE `company_ticker` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `symbol` varchar(20) NOT NULL COMMENT 'symbol of stock, such as ''TSLA''',
  `name` varchar(255) NOT NULL COMMENT 'name of stock',
  `currency` varchar(20) NOT NULL COMMENT 'such as ''USD''',
  `stock_exchange` varchar(100) NOT NULL COMMENT 'such as ''NasdaqGS''',
  `exchange_short` varchar(20) NOT NULL COMMENT 'such as ''NASDAQ''',
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_symbol_exchange` (`symbol`,`stock_exchange`) COMMENT 'unique symbol in a stock exchange'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SET FOREIGN_KEY_CHECKS = 1;
