-- =========================================================
-- 场景化合规系统：全量数据库初始化脚本
-- 适用：MySQL 8.x
-- 执行方式：
--   mysql -u root -p < db_init_all_tables.sql
-- =========================================================

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

CREATE DATABASE IF NOT EXISTS `beijing_demo4`
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_ai_ci;

USE `beijing_demo4`;

-- =========================================================
-- 1) 知识库主文档表
-- =========================================================
CREATE TABLE IF NOT EXISTS `kb_document` (
  `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `title` VARCHAR(255) NOT NULL COMMENT '文档标题',
  `doc_type` VARCHAR(50) DEFAULT NULL COMMENT '文档类型',
  `status` VARCHAR(20) DEFAULT '有效' COMMENT '状态',
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_title` (`title`),
  KEY `idx_status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='知识库文档表';

-- =========================================================
-- 2) 知识库分段表
-- =========================================================
CREATE TABLE IF NOT EXISTS `kb_chunk` (
  `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `document_id` BIGINT NOT NULL COMMENT '所属文档ID',
  `chunk_no` INT NOT NULL COMMENT '文档内顺序号',
  `section_no` VARCHAR(50) DEFAULT NULL COMMENT '章节编号',
  `section_title` VARCHAR(255) DEFAULT NULL COMMENT '章节标题',
  `content` LONGTEXT NOT NULL COMMENT '正文内容',
  `page_no` INT DEFAULT NULL COMMENT '页码',
  `parent_section` VARCHAR(50) DEFAULT NULL COMMENT '父章节编号',
  `is_title` INT DEFAULT 0 COMMENT '是否标题(1是/0否)',
  `level_no` INT DEFAULT NULL COMMENT '层级',
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_doc_id` (`document_id`),
  KEY `idx_doc_chunkno` (`document_id`, `chunk_no`),
  KEY `idx_section_no` (`section_no`),
  KEY `idx_parent_section` (`parent_section`),
  KEY `idx_doc_section` (`document_id`, `section_no`),
  CONSTRAINT `fk_kb_chunk_document`
    FOREIGN KEY (`document_id`) REFERENCES `kb_document` (`id`)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='知识库分段表';

-- =========================================================
-- 3) 来源文档表
-- =========================================================
-- =========================================================
-- kb_chunk v4 schema normalization (section/subsection model)
-- =========================================================
ALTER TABLE `kb_chunk`
  DROP INDEX IF EXISTS `idx_parent_section`;

ALTER TABLE `kb_chunk`
  DROP INDEX IF EXISTS `idx_subsection_no`;

ALTER TABLE `kb_chunk`
  DROP INDEX IF EXISTS `idx_doc_subsection`;

ALTER TABLE `kb_chunk`
  ADD COLUMN IF NOT EXISTS `subsection_no` VARCHAR(50) DEFAULT NULL COMMENT '灏忚妭缂栧彿' AFTER `section_title`,
  ADD COLUMN IF NOT EXISTS `subsection_title` VARCHAR(255) DEFAULT NULL COMMENT '灏忚妭鏍囬' AFTER `subsection_no`;

ALTER TABLE `kb_chunk`
  DROP COLUMN IF EXISTS `parent_section`,
  DROP COLUMN IF EXISTS `is_title`,
  DROP COLUMN IF EXISTS `level_no`,
  DROP COLUMN IF EXISTS `updated_at`;

ALTER TABLE `kb_chunk`
  ADD INDEX `idx_subsection_no` (`subsection_no`);

ALTER TABLE `kb_chunk`
  ADD INDEX `idx_doc_subsection` (`document_id`, `subsection_no`);

CREATE TABLE IF NOT EXISTS `biz_source_documents` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `file_name` VARCHAR(255) NOT NULL,
  `title` VARCHAR(512) NOT NULL,
  `content_text` LONGTEXT NOT NULL,
  `content_hash` VARCHAR(64) NOT NULL,
  `category` ENUM('policy', 'case', 'speech') DEFAULT 'policy',
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_biz_source_documents_content_hash` (`content_hash`),
  KEY `ix_biz_source_documents_category` (`category`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =========================================================
-- 4) 法律知识点表（可选能力，建议保留）
-- =========================================================
CREATE TABLE IF NOT EXISTS `biz_legal_knowledge` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `source_doc_id` BIGINT NOT NULL,
  `knowledge_type` ENUM('spirit', 'principle') NOT NULL,
  `name` VARCHAR(255) NOT NULL,
  `description` TEXT NOT NULL,
  `golden_quote` TEXT DEFAULT NULL,
  `domain` VARCHAR(100) DEFAULT NULL,
  `is_indexed` TINYINT(1) DEFAULT 0,
  `content_hash` VARCHAR(64) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_biz_legal_knowledge_content_hash` (`content_hash`),
  KEY `idx_knowledge_query` (`knowledge_type`, `is_indexed`),
  KEY `ix_biz_legal_knowledge_domain` (`domain`),
  CONSTRAINT `fk_biz_legal_knowledge_source_doc`
    FOREIGN KEY (`source_doc_id`) REFERENCES `biz_source_documents` (`id`)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =========================================================
-- 5) 历史评估结果表
-- =========================================================
CREATE TABLE IF NOT EXISTS `biz_risk_assessments` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `event_summary` TEXT NOT NULL,
  `risk_level` VARCHAR(20) NOT NULL,
  `total_score` INT NOT NULL,
  `full_report_md` TEXT DEFAULT NULL,
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =========================================================
-- 6) 业务类型表
-- =========================================================
CREATE TABLE IF NOT EXISTS `biz_business_type` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `type_code` VARCHAR(64) NOT NULL,
  `type_name` VARCHAR(128) NOT NULL,
  `parent_id` BIGINT DEFAULT NULL,
  `description` VARCHAR(500) DEFAULT NULL,
  `status` INT NOT NULL DEFAULT 1,
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ix_biz_business_type_type_code` (`type_code`),
  KEY `ix_biz_business_type_type_name` (`type_name`),
  KEY `ix_biz_business_type_status` (`status`),
  KEY `ix_biz_business_type_parent_id` (`parent_id`),
  CONSTRAINT `fk_biz_business_type_parent`
    FOREIGN KEY (`parent_id`) REFERENCES `biz_business_type` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =========================================================
-- 7) 制度文档表
-- =========================================================
CREATE TABLE IF NOT EXISTS `biz_policy_document` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `doc_code` VARCHAR(64) NOT NULL,
  `doc_name` VARCHAR(255) NOT NULL,
  `doc_category` VARCHAR(64) NOT NULL DEFAULT 'other',
  `version_no` VARCHAR(32) NOT NULL DEFAULT 'v1.0',
  `issuing_dept` VARCHAR(128) DEFAULT NULL,
  `effective_date` DATE DEFAULT NULL,
  `expiry_date` DATE DEFAULT NULL,
  `status` INT NOT NULL DEFAULT 1,
  `source_doc_id` BIGINT DEFAULT NULL,
  `source_uri` VARCHAR(500) DEFAULT NULL,
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ix_biz_policy_document_doc_code` (`doc_code`),
  KEY `ix_biz_policy_document_doc_name` (`doc_name`),
  KEY `ix_biz_policy_document_doc_category` (`doc_category`),
  KEY `ix_biz_policy_document_status` (`status`),
  KEY `ix_biz_policy_document_source_doc_id` (`source_doc_id`),
  CONSTRAINT `fk_biz_policy_document_source_doc`
    FOREIGN KEY (`source_doc_id`) REFERENCES `biz_source_documents` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =========================================================
-- 8) 制度条款表
-- =========================================================
CREATE TABLE IF NOT EXISTS `biz_policy_clause` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `policy_doc_id` BIGINT NOT NULL,
  `clause_no` VARCHAR(64) NOT NULL,
  `clause_title` VARCHAR(255) DEFAULT NULL,
  `clause_text` LONGTEXT NOT NULL,
  `page_no` INT DEFAULT NULL,
  `anchor_code` VARCHAR(128) DEFAULT NULL,
  `content_hash` VARCHAR(64) NOT NULL,
  `status` INT NOT NULL DEFAULT 1,
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_biz_policy_clause_content_hash` (`content_hash`),
  KEY `ix_biz_policy_clause_policy_doc_id` (`policy_doc_id`),
  KEY `ix_biz_policy_clause_clause_no` (`clause_no`),
  KEY `ix_biz_policy_clause_status` (`status`),
  CONSTRAINT `fk_biz_policy_clause_policy_doc`
    FOREIGN KEY (`policy_doc_id`) REFERENCES `biz_policy_document` (`id`)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =========================================================
-- 9) 业务-条款映射表
-- =========================================================
CREATE TABLE IF NOT EXISTS `biz_business_clause_map` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `business_type_id` BIGINT NOT NULL,
  `clause_id` BIGINT NOT NULL,
  `mandatory_level` INT NOT NULL DEFAULT 1,
  `relevance_weight` FLOAT NOT NULL DEFAULT 1.0,
  `trigger_keywords` TEXT DEFAULT NULL,
  `remark` VARCHAR(500) DEFAULT NULL,
  `status` INT NOT NULL DEFAULT 1,
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_biz_type_clause` (`business_type_id`, `clause_id`),
  KEY `ix_biz_business_clause_map_business_type_id` (`business_type_id`),
  KEY `ix_biz_business_clause_map_clause_id` (`clause_id`),
  KEY `ix_biz_business_clause_map_status` (`status`),
  CONSTRAINT `fk_biz_business_clause_map_business_type`
    FOREIGN KEY (`business_type_id`) REFERENCES `biz_business_type` (`id`)
    ON DELETE CASCADE,
  CONSTRAINT `fk_biz_business_clause_map_clause`
    FOREIGN KEY (`clause_id`) REFERENCES `biz_policy_clause` (`id`)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =========================================================
-- 10) 流程步骤表
-- =========================================================
CREATE TABLE IF NOT EXISTS `biz_procedure_step` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `business_type_id` BIGINT NOT NULL,
  `step_no` INT NOT NULL,
  `step_name` VARCHAR(255) NOT NULL,
  `step_desc` TEXT NOT NULL,
  `responsible_role` VARCHAR(128) DEFAULT NULL,
  `due_rule` VARCHAR(255) DEFAULT NULL,
  `output_deliverable` VARCHAR(255) DEFAULT NULL,
  `clause_id` BIGINT DEFAULT NULL,
  `status` INT NOT NULL DEFAULT 1,
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_biz_type_step` (`business_type_id`, `step_no`),
  KEY `ix_biz_procedure_step_business_type_id` (`business_type_id`),
  KEY `ix_biz_procedure_step_clause_id` (`clause_id`),
  KEY `ix_biz_procedure_step_status` (`status`),
  CONSTRAINT `fk_biz_procedure_step_business_type`
    FOREIGN KEY (`business_type_id`) REFERENCES `biz_business_type` (`id`)
    ON DELETE CASCADE,
  CONSTRAINT `fk_biz_procedure_step_clause`
    FOREIGN KEY (`clause_id`) REFERENCES `biz_policy_clause` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =========================================================
-- 11) 必需材料表
-- =========================================================
CREATE TABLE IF NOT EXISTS `biz_required_material` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `business_type_id` BIGINT NOT NULL,
  `material_code` VARCHAR(64) NOT NULL,
  `material_name` VARCHAR(255) NOT NULL,
  `required_level` INT NOT NULL DEFAULT 1,
  `format_rule` VARCHAR(255) DEFAULT NULL,
  `validator_rule` TEXT DEFAULT NULL,
  `clause_id` BIGINT DEFAULT NULL,
  `status` INT NOT NULL DEFAULT 1,
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_biz_type_material` (`business_type_id`, `material_code`),
  KEY `ix_biz_required_material_business_type_id` (`business_type_id`),
  KEY `ix_biz_required_material_clause_id` (`clause_id`),
  KEY `ix_biz_required_material_status` (`status`),
  CONSTRAINT `fk_biz_required_material_business_type`
    FOREIGN KEY (`business_type_id`) REFERENCES `biz_business_type` (`id`)
    ON DELETE CASCADE,
  CONSTRAINT `fk_biz_required_material_clause`
    FOREIGN KEY (`clause_id`) REFERENCES `biz_policy_clause` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =========================================================
-- 12) 合规任务主表
-- =========================================================
CREATE TABLE IF NOT EXISTS `biz_compliance_task` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `task_no` VARCHAR(64) NOT NULL,
  `input_mode` VARCHAR(32) NOT NULL DEFAULT 'text',
  `input_text` LONGTEXT DEFAULT NULL,
  `uploaded_doc_id` BIGINT DEFAULT NULL,
  `applicant_id` VARCHAR(64) DEFAULT NULL,
  `applicant_dept` VARCHAR(128) DEFAULT NULL,
  `model_version` VARCHAR(64) DEFAULT NULL,
  `overall_score` FLOAT DEFAULT NULL,
  `compliance_status` VARCHAR(32) DEFAULT NULL,
  `risk_level` VARCHAR(20) DEFAULT NULL,
  `status` INT NOT NULL DEFAULT 1,
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `completed_at` DATETIME DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ix_biz_compliance_task_task_no` (`task_no`),
  KEY `ix_biz_compliance_task_uploaded_doc_id` (`uploaded_doc_id`),
  KEY `ix_biz_compliance_task_status` (`status`),
  KEY `ix_biz_compliance_task_created_at` (`created_at`),
  CONSTRAINT `fk_biz_compliance_task_uploaded_doc`
    FOREIGN KEY (`uploaded_doc_id`) REFERENCES `biz_source_documents` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =========================================================
-- 13) 任务命中业务类型表
-- =========================================================
CREATE TABLE IF NOT EXISTS `biz_task_type_hit` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `task_id` BIGINT NOT NULL,
  `business_type_id` BIGINT NOT NULL,
  `confidence` FLOAT NOT NULL DEFAULT 0.0,
  `evidence_text` VARCHAR(1000) DEFAULT NULL,
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `ix_biz_task_type_hit_task_id` (`task_id`),
  KEY `ix_biz_task_type_hit_business_type_id` (`business_type_id`),
  CONSTRAINT `fk_biz_task_type_hit_task`
    FOREIGN KEY (`task_id`) REFERENCES `biz_compliance_task` (`id`)
    ON DELETE CASCADE,
  CONSTRAINT `fk_biz_task_type_hit_business_type`
    FOREIGN KEY (`business_type_id`) REFERENCES `biz_business_type` (`id`)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =========================================================
-- 14) 任务缺漏项表
-- =========================================================
CREATE TABLE IF NOT EXISTS `biz_task_gap` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `task_id` BIGINT NOT NULL,
  `gap_type` VARCHAR(32) NOT NULL,
  `gap_item` VARCHAR(255) NOT NULL,
  `expected_req` TEXT NOT NULL,
  `detected_content` TEXT DEFAULT NULL,
  `severity` INT NOT NULL DEFAULT 2,
  `fix_suggestion` TEXT DEFAULT NULL,
  `clause_id` BIGINT DEFAULT NULL,
  `trace_link` VARCHAR(500) DEFAULT NULL,
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `ix_biz_task_gap_task_id` (`task_id`),
  KEY `ix_biz_task_gap_clause_id` (`clause_id`),
  CONSTRAINT `fk_biz_task_gap_task`
    FOREIGN KEY (`task_id`) REFERENCES `biz_compliance_task` (`id`)
    ON DELETE CASCADE,
  CONSTRAINT `fk_biz_task_gap_clause`
    FOREIGN KEY (`clause_id`) REFERENCES `biz_policy_clause` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SET FOREIGN_KEY_CHECKS = 1;
