/*
Navicat MySQL Data Transfer

Source Server         : hadoop
Source Server Version : 50173
Source Host           : hadoop:3306
Source Database       : bill

Target Server Type    : MYSQL
Target Server Version : 50173
File Encoding         : 65001

Date: 2017-10-17 13:15:12
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for bill
-- ----------------------------
DROP TABLE IF EXISTS `bill`;
CREATE TABLE `bill` (
  `b_key` varchar(50) DEFAULT NULL,
  `b_value` varchar(1500) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;
