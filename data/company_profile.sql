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

 Date: 19/02/2024 23:39:55
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for company_profile
-- ----------------------------
DROP TABLE IF EXISTS `company_profile`;
CREATE TABLE `company_profile` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `symbol` varchar(20) NOT NULL COMMENT 'symbol of stock, such as ''TSLA''',
  `name` varchar(255) NOT NULL COMMENT 'name of stock',
  `currency` varchar(20) NOT NULL COMMENT 'such as ''USD''',
  `stock_exchange` varchar(100) NOT NULL COMMENT 'such as ''NasdaqGS''',
  `exchange_short` varchar(20) NOT NULL COMMENT 'such as ''NASDAQ''',
  `price` decimal(10,2) unsigned DEFAULT '0.00',
  `beta` decimal(10,6) unsigned DEFAULT '0.000000',
  `vol_avg` bigint unsigned DEFAULT '0',
  `mkt_cap` bigint unsigned DEFAULT '0',
  `last_div` decimal(10,2) DEFAULT NULL,
  `range_1` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `changes` decimal(10,2) DEFAULT '0.00',
  `cik` varchar(50) DEFAULT NULL,
  `isin` varchar(50) DEFAULT NULL,
  `cusip` varchar(50) DEFAULT NULL,
  `industry` varchar(100) DEFAULT NULL,
  `website` varchar(100) DEFAULT NULL,
  `description` varchar(10000) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `ceo` varchar(50) DEFAULT NULL,
  `sector` varchar(50) DEFAULT NULL,
  `country` varchar(50) DEFAULT NULL,
  `fulltime_employees` int unsigned DEFAULT '0',
  `phone` varchar(50) DEFAULT NULL,
  `address` varchar(255) DEFAULT NULL,
  `city` varchar(50) DEFAULT NULL,
  `state` varchar(50) DEFAULT NULL,
  `zip` varchar(20) DEFAULT NULL,
  `dcf_diff` decimal(10,5) DEFAULT NULL,
  `dcf` double(20,14) DEFAULT NULL,
  `image` varchar(255) DEFAULT NULL,
  `ipo_date` datetime DEFAULT NULL,
  `default_image` tinyint unsigned DEFAULT '0',
  `is_etf` tinyint unsigned DEFAULT '0',
  `is_active_trading` tinyint unsigned DEFAULT '0',
  `is_adr` tinyint unsigned DEFAULT '0',
  `is_fund` tinyint unsigned DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_symbol_exchange` (`symbol`,`stock_exchange`) COMMENT 'unique symbol in a stock exchange'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SET FOREIGN_KEY_CHECKS = 1;
