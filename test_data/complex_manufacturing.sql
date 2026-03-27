-- Complex Manufacturing Database with Cryptic Column Names
-- This will challenge the AI agent's pattern recognition abilities

-- Work Center Master Table
CREATE TABLE work_centers (
    wc_ref VARCHAR(8) PRIMARY KEY,
    wc_name VARCHAR(50) NOT NULL,
    department VARCHAR(20),
    cost_center VARCHAR(8),
    capacity_units_hr DECIMAL(8,2),
    efficiency_pct DECIMAL(5,2) DEFAULT 100.00,
    utilization_target_pct DECIMAL(5,2) DEFAULT 85.00,
    setup_crew_size INTEGER DEFAULT 1,
    operator_crew_size INTEGER DEFAULT 1,
    shift_pattern VARCHAR(10),
    status CHAR(1) CHECK (status IN ('A','I','M','D')), -- Active, Inactive, Maintenance, Decommissioned
    created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Employee Master Table
CREATE TABLE employees (
    badge_id VARCHAR(10) PRIMARY KEY,
    emp_name VARCHAR(100) NOT NULL,
    job_title VARCHAR(50),
    department VARCHAR(20),
    skill_level INTEGER CHECK (skill_level BETWEEN 1 AND 5),
    certification_codes TEXT,
    hourly_rate DECIMAL(8,2),
    shift_assignment CHAR(1) CHECK (shift_assignment IN ('A','B','C','D')),
    supervisor_badge VARCHAR(10),
    hire_date DATE,
    status CHAR(1) CHECK (status IN ('A','I','T','L')), -- Active, Inactive, Terminated, Leave
    created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (supervisor_badge) REFERENCES employees(badge_id)
);

-- Part Master Table
CREATE TABLE part_master (
    part_num VARCHAR(25) PRIMARY KEY,
    part_desc VARCHAR(200),
    part_type VARCHAR(20),
    product_family VARCHAR(30),
    make_buy_code CHAR(1) CHECK (make_buy_code IN ('M','B','O')), -- Make, Buy, Outside Process
    unit_of_measure VARCHAR(6),
    std_cost DECIMAL(10,4),
    weight_kg DECIMAL(8,4),
    material_type VARCHAR(30),
    supplier_pref VARCHAR(20),
    lead_time_days INTEGER,
    safety_stock_qty INTEGER,
    abc_class CHAR(1) CHECK (abc_class IN ('A','B','C')),
    status CHAR(1) CHECK (status IN ('A','O','D')), -- Active, Obsolete, Discontinued
    created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE prod_sched_mx (
    psmx_id VARCHAR(16) PRIMARY KEY,
    wc_ref VARCHAR(8) NOT NULL REFERENCES work_centers(wc_ref),
    part_num VARCHAR(25) REFERENCES part_master(part_num),
    op_seq_num INTEGER NOT NULL,
    std_cyc_tm_sec INTEGER,
    act_cyc_tm_sec INTEGER,
    setup_tm_min INTEGER,
    teardown_tm_min INTEGER,
    eff_pct DECIMAL(5,2),
    oee_calc DECIMAL(5,2),
    scrap_qty INTEGER DEFAULT 0,
    rework_qty INTEGER DEFAULT 0,
    good_qty INTEGER,
    batch_sz INTEGER,
    lot_trk_id VARCHAR(20),
    qual_stat CHAR(1) CHECK (qual_stat IN ('A','R','H','P')),
    insp_req BOOLEAN DEFAULT FALSE,
    cert_req BOOLEAN DEFAULT FALSE,
    spc_ctrl BOOLEAN DEFAULT FALSE,
    temp_log_req BOOLEAN DEFAULT FALSE,
    press_log_req BOOLEAN DEFAULT FALSE,
    vib_mon_req BOOLEAN DEFAULT FALSE,
    tool_wear_idx DECIMAL(4,2),
    maint_due_hrs INTEGER,
    last_pm_ts TIMESTAMP,
    next_pm_ts TIMESTAMP,
    downtime_min INTEGER DEFAULT 0,
    reason_cd VARCHAR(4),
    shift_cd CHAR(1),
    operator_badge VARCHAR(10) REFERENCES employees(badge_id),
    supervisor_badge VARCHAR(10) REFERENCES employees(badge_id),
    created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE matl_mvmt_log (
    mvmt_id VARCHAR(20) PRIMARY KEY,
    part_num VARCHAR(25) NOT NULL REFERENCES part_master(part_num),
    lot_batch_id VARCHAR(18),
    serial_num VARCHAR(30),
    from_loc VARCHAR(12),
    to_loc VARCHAR(12),
    mvmt_type CHAR(2) CHECK (mvmt_type IN ('IS','TR','WO','SH','RT','SC','AD')),
    qty_moved DECIMAL(12,4),
    uom VARCHAR(6),
    unit_cost DECIMAL(10,4),
    ext_cost DECIMAL(15,2),
    reason_cd VARCHAR(6),
    ref_doc_num VARCHAR(20),
    ref_doc_line INTEGER,
    gl_acct VARCHAR(15),
    cost_center VARCHAR(8),
    project_cd VARCHAR(12),
    wbs_element VARCHAR(24),
    reservation_num VARCHAR(12),
    goods_mvmt_cd VARCHAR(4),
    plant_cd VARCHAR(4),
    storage_loc VARCHAR(4),
    special_stock CHAR(1),
    vendor_batch VARCHAR(15),
    mfg_date DATE,
    exp_date DATE,
    shelf_life_days INTEGER,
    temp_zone CHAR(1),
    hazmat_class VARCHAR(4),
    un_num VARCHAR(6),
    msds_ref VARCHAR(15),
    created_by VARCHAR(12) REFERENCES employees(badge_id),
    created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    posted_ts TIMESTAMP,
    reversed_flag BOOLEAN DEFAULT FALSE
);

CREATE TABLE qc_insp_res (
    insp_id VARCHAR(18) PRIMARY KEY,
    lot_id VARCHAR(20) NOT NULL,
    insp_plan VARCHAR(12),
    char_id VARCHAR(15),
    char_desc VARCHAR(100),
    target_val DECIMAL(15,6),
    usl_val DECIMAL(15,6),
    lsl_val DECIMAL(15,6),
    measured_val DECIMAL(15,6),
    tolerance_pct DECIMAL(5,2),
    cp_val DECIMAL(6,3),
    cpk_val DECIMAL(6,3),
    pp_val DECIMAL(6,3),
    ppk_val DECIMAL(6,3),
    sigma_level DECIMAL(4,2),
    defect_rate_ppm INTEGER,
    sample_sz INTEGER,
    subgroup_sz INTEGER,
    freq_hrs INTEGER,
    insp_method VARCHAR(8),
    gage_id VARCHAR(12),
    gage_cal_due DATE,
    r_and_r_pct DECIMAL(5,2),
    bias_pct DECIMAL(5,2),
    linearity_pct DECIMAL(5,2),
    stability_pct DECIMAL(5,2),
    result_stat CHAR(1) CHECK (result_stat IN ('P','F','W','H')),
    ncr_num VARCHAR(15),
    corrective_action TEXT,
    preventive_action TEXT,
    inspector_badge VARCHAR(10) REFERENCES employees(badge_id),
    insp_ts TIMESTAMP,
    approved_by VARCHAR(10) REFERENCES employees(badge_id),
    approved_ts TIMESTAMP,
    cert_num VARCHAR(20),
    customer_notif BOOLEAN DEFAULT FALSE
);

CREATE TABLE equip_maint_hist (
    maint_id VARCHAR(16) PRIMARY KEY,
    equip_tag VARCHAR(15) NOT NULL,
    wo_num VARCHAR(12),
    maint_type CHAR(2) CHECK (maint_type IN ('PM','CM','PD','EM','CB')),
    priority_lvl INTEGER CHECK (priority_lvl BETWEEN 1 AND 5),
    planner_group VARCHAR(6),
    work_center VARCHAR(8) REFERENCES work_centers(wc_ref),
    func_loc VARCHAR(30),
    sys_status VARCHAR(4),
    user_status VARCHAR(4),
    notification_num VARCHAR(12),
    breakdown_flag BOOLEAN DEFAULT FALSE,
    safety_critical BOOLEAN DEFAULT FALSE,
    env_critical BOOLEAN DEFAULT FALSE,
    prod_critical BOOLEAN DEFAULT FALSE,
    est_duration_hrs DECIMAL(6,2),
    act_duration_hrs DECIMAL(6,2),
    est_cost DECIMAL(12,2),
    act_cost DECIMAL(12,2),
    labor_hrs DECIMAL(6,2),
    labor_cost DECIMAL(10,2),
    material_cost DECIMAL(10,2),
    contractor_cost DECIMAL(10,2),
    overhead_cost DECIMAL(10,2),
    downtime_hrs DECIMAL(6,2),
    prod_loss_units INTEGER,
    prod_loss_value DECIMAL(12,2),
    mttr_hrs DECIMAL(6,2),
    mtbf_hrs DECIMAL(8,2),
    availability_pct DECIMAL(5,2),
    reliability_idx DECIMAL(4,2),
    maint_strategy VARCHAR(8),
    next_due_date DATE,
    next_due_counter INTEGER,
    counter_reading INTEGER,
    counter_uom VARCHAR(6),
    failure_mode VARCHAR(50),
    root_cause VARCHAR(100),
    technician_badge VARCHAR(10) REFERENCES employees(badge_id),
    supervisor_badge VARCHAR(10) REFERENCES employees(badge_id),
    started_ts TIMESTAMP,
    completed_ts TIMESTAMP,
    created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert master data first
INSERT INTO work_centers VALUES 
('WC001', 'CNC Machining Center 1', 'MACHINING', 'CC001', 24.00, 95.50, 85.00, 1, 2, '3SHIFT', 'A', '2024-01-01 00:00:00'),
('WC002', 'Assembly Line A', 'ASSEMBLY', 'CC002', 48.00, 88.75, 90.00, 2, 4, '2SHIFT', 'A', '2024-01-01 00:00:00'),
('WC003', 'Quality Control Station', 'QC', 'CC003', 16.00, 92.25, 80.00, 1, 1, '1SHIFT', 'A', '2024-01-01 00:00:00');

INSERT INTO employees VALUES 
('EMP001', 'John Smith', 'CNC Operator', 'MACHINING', 4, 'CNC-L3,SAFETY-L2', 28.50, 'A', 'SUP001', '2020-03-15', 'A', '2024-01-01 00:00:00'),
('EMP002', 'Maria Garcia', 'Assembly Technician', 'ASSEMBLY', 3, 'ASM-L3,ELEC-L2', 24.75, 'B', 'SUP001', '2021-07-22', 'A', '2024-01-01 00:00:00'),
('EMP003', 'David Chen', 'QC Inspector', 'QC', 5, 'QC-L5,CMM-L4,STAT-L3', 32.25, 'A', 'SUP002', '2019-11-08', 'A', '2024-01-01 00:00:00'),
('SUP001', 'Sarah Johnson', 'Production Supervisor', 'PRODUCTION', 5, 'MGMT-L5,LEAN-L4', 45.00, 'A', NULL, '2018-05-12', 'A', '2024-01-01 00:00:00'),
('SUP002', 'Michael Brown', 'Quality Manager', 'QC', 5, 'QC-L5,MGMT-L4,ISO9001', 48.75, 'A', NULL, '2017-09-03', 'A', '2024-01-01 00:00:00');

INSERT INTO part_master VALUES 
('PN-A7B9C2D4E6F8', 'Precision Bearing Assembly', 'COMPONENT', 'BEARINGS', 'M', 'EA', 125.7500, 2.4500, 'STEEL_ALLOY', 'ACME_BEARINGS', 14, 50, 'A', 'A', '2024-01-01 00:00:00'),
('PN-B8C0D3E5F7G9', 'Electronic Control Module', 'ASSEMBLY', 'ELECTRONICS', 'B', 'EA', 875.2500, 0.8750, 'PCB_ASSEMBLY', 'TECH_SOLUTIONS', 21, 25, 'A', 'A', '2024-01-01 00:00:00'),
('PN-C9D1E4F6G8H0', 'Hydraulic Cylinder Rod', 'MACHINED', 'HYDRAULICS', 'M', 'EA', 245.5000, 12.5000, 'STAINLESS_316', 'INTERNAL', 7, 15, 'B', 'A', '2024-01-01 00:00:00');

-- Insert production schedule data with proper relationships
INSERT INTO prod_sched_mx VALUES 
('PSMX240115001', 'WC001', 'PN-A7B9C2D4E6F8', 10, 450, 467, 30, 15, 94.2, 89.7, 2, 1, 98, 100, 'LOT2024A001', 'A', TRUE, FALSE, TRUE, TRUE, FALSE, TRUE, 2.3, 120, '2024-01-10 08:00:00', '2024-02-10 08:00:00', 0, NULL, 'A', 'EMP001', 'SUP001', '2024-01-15 06:30:00', '2024-01-15 06:30:00'),
('PSMX240115002', 'WC002', 'PN-B8C0D3E5F7G9', 20, 380, 395, 45, 20, 91.8, 87.3, 3, 0, 97, 100, 'LOT2024A002', 'P', FALSE, TRUE, FALSE, FALSE, TRUE, FALSE, 3.1, 80, '2024-01-08 14:00:00', '2024-02-08 14:00:00', 15, 'BRK', 'B', 'EMP002', 'SUP001', '2024-01-15 07:15:00', '2024-01-15 07:15:00'),
('PSMX240115003', 'WC003', 'PN-C9D1E4F6G8H0', 30, 520, 548, 25, 10, 96.7, 92.1, 1, 2, 97, 100, 'LOT2024A003', 'H', TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, 1.8, 200, '2024-01-12 10:00:00', '2024-02-12 10:00:00', 28, 'MTL', 'C', 'EMP003', 'SUP002', '2024-01-15 08:45:00', '2024-01-15 08:45:00');

INSERT INTO matl_mvmt_log VALUES 
('MVMT240115001', 'PN-A7B9C2D4E6F8', 'BATCH2024001', 'SN240115001', 'WH-A-01-02', 'WC001-IN', 'IS', 50.0000, 'EA', 12.5000, 625.00, 'PROD', 'WO240115001', 1, '500100-001', 'CC001', 'PRJ-ALPHA', 'WBS-001-001', 'RES001', '101', 'P001', 'SL01', NULL, 'VB001', '2024-01-10', '2024-07-10', 180, 'A', NULL, NULL, NULL, 'EMP001', '2024-01-15 06:00:00', '2024-01-15 06:05:00', FALSE),
('MVMT240115002', 'PN-B8C0D3E5F7G9', 'BATCH2024002', NULL, 'WC001-OUT', 'WH-B-02-03', 'TR', 48.0000, 'EA', 15.7500, 756.00, 'COMP', 'WO240115001', 2, '500200-002', 'CC002', 'PRJ-BETA', 'WBS-002-001', 'RES002', '261', 'P001', 'SL02', NULL, NULL, '2024-01-15', NULL, NULL, 'B', 'HAZ3', 'UN1234', 'MSDS001', 'EMP002', '2024-01-15 08:30:00', '2024-01-15 08:35:00', FALSE);

INSERT INTO qc_insp_res VALUES 
('INSP240115001', 'LOT2024A001', 'QP-STD-001', 'CHAR-DIM-001', 'Overall Length Dimension +/- 0.05mm', 25.0000, 25.0500, 24.9500, 25.0120, 0.20, 1.250, 1.180, 1.310, 1.220, 4.25, 150, 30, 5, 4, 'CMM-001', 'CMM001', '2024-03-15', 8.5, 2.1, 1.8, 3.2, 'P', NULL, NULL, NULL, 'EMP003', '2024-01-15 09:15:00', 'SUP002', '2024-01-15 10:00:00', 'CERT240115001', FALSE),
('INSP240115002', 'LOT2024A002', 'QP-STD-002', 'CHAR-SURF-001', 'Surface Roughness Ra 1.6 max', 1.6000, 1.6000, 0.0000, 1.4500, 10.00, 1.890, 1.750, 2.100, 1.920, 3.85, 280, 25, 5, 2, 'SURF-001', 'SURF001', '2024-02-28', 12.3, 3.8, 2.5, 4.1, 'P', NULL, NULL, NULL, 'EMP003', '2024-01-15 11:30:00', 'SUP002', '2024-01-15 12:15:00', 'CERT240115002', FALSE);

INSERT INTO equip_maint_hist VALUES 
('MAINT240115001', 'EQ-CNC-001', 'WO-M-001', 'PM', 2, 'PLN01', 'WC001', 'PLANT-A/SHOP-1/LINE-1/CNC-001', 'REL', 'COMP', 'NOTIF001', FALSE, TRUE, FALSE, TRUE, 4.00, 4.25, 850.00, 892.50, 4.25, 170.00, 325.50, 0.00, 397.00, 0.00, 0, 0.00, 4.25, 168.5, 98.7, 4.2, 'RCM', '2024-02-15', 1500, 1487, 'HRS', NULL, NULL, 'EMP001', 'SUP001', '2024-01-15 06:00:00', '2024-01-15 10:15:00', '2024-01-14 16:00:00'),
('MAINT240115002', 'EQ-MILL-002', 'WO-M-002', 'CM', 1, 'PLN02', 'WC002', 'PLANT-A/SHOP-1/LINE-2/MILL-002', 'TECO', 'COMP', 'NOTIF002', TRUE, FALSE, TRUE, TRUE, 6.00, 8.50, 1250.00, 1687.25, 8.50, 340.00, 892.75, 454.50, 0.00, 8.50, 425, 12750.00, 8.50, 142.3, 94.2, 3.8, 'RTF', '2024-01-22', NULL, NULL, NULL, 'Bearing Failure', 'Inadequate Lubrication Schedule', 'EMP002', 'SUP001', '2024-01-15 14:30:00', '2024-01-15 23:00:00', '2024-01-15 13:45:00');
