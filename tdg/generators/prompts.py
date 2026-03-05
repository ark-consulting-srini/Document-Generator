"""
LLM Prompt Templates for T - BRD STTM Generator

Centralized location for all prompt templates used with Snowflake Cortex LLMs.
"""


def create_raw_xml_brd_prompt(raw_xml_summary: str, 
                              business_context: str = "",
                              additional_requirements: str = "") -> str:
    """
    Create a prompt for generating BRD from raw XML structure.
    
    Args:
        raw_xml_summary: Summary of the XML mapping structure
        business_context: Optional business context
        additional_requirements: Optional additional requirements
        
    Returns:
        Formatted prompt string
    """
    prompt = f"""You are a Business Analyst expert in ETL processes and data warehousing.

Based on the following RAW INFORMATICA XML MAPPING STRUCTURE, generate a comprehensive Business Requirements Document (BRD).

{f"BUSINESS CONTEXT:{chr(10)}{business_context}{chr(10)}" if business_context else ""}

{f"ADDITIONAL REQUIREMENTS:{chr(10)}{additional_requirements}{chr(10)}" if additional_requirements else ""}

{raw_xml_summary}

Please analyze this raw XML structure and generate the BRD with the following sections:

## 1. Overview
Brief description of what this ETL process does based on the mapping structure.

## 2. Source System Analysis
Describe each source table, its purpose, and key fields based on the SOURCE DEFINITIONS.

## 3. Target System Analysis
Describe each target table, its purpose, and key fields based on the TARGET DEFINITIONS.

## 4. Business Functional Requirements (BRD)
List each requirement with a unique ID in format BR-1, BR-2, etc.
- BR-1: [Requirement]
- BR-2: [Requirement]
(Continue for all requirements)

## 5. Transformation Logic
Describe the key transformations, expressions, lookups, and their business purpose.

## 6. Data Quality Requirements
List data quality requirements with ID format DQ-1, DQ-2, etc.
- DQ-1: [Requirement]
- DQ-2: [Requirement]

## 7. Technical Notes
Any SQL overrides, filters, or special technical considerations found in the mapping.

Format the output in clear markdown sections."""

    return prompt


def create_requirements_prompt(lineage_summary: str, 
                               business_context: str = "",
                               additional_requirements: str = "",
                               model_role: str = "primary") -> str:
    """
    Create a prompt for generating business functional requirements in BRD format.
    
    Args:
        lineage_summary: Summary of the lineage data
        business_context: Optional business context
        additional_requirements: Optional additional requirements
        model_role: Role of the model (primary/secondary)
        
    Returns:
        Formatted prompt string
    """
    prompt = f"""You are a Business Analyst expert in ETL processes and data warehousing.

Based on the following ETL mapping information, generate a comprehensive Business Requirements Document (BRD).

{f"BUSINESS CONTEXT:{chr(10)}{business_context}{chr(10)}" if business_context else ""}

{f"ADDITIONAL REQUIREMENTS:{chr(10)}{additional_requirements}{chr(10)}" if additional_requirements else ""}

ETL MAPPING SUMMARY:
{lineage_summary}

Please generate the BRD with the following sections:

## 1. Overview
Brief description of what this ETL process does, its purpose, and business value.

## 2. Business Functional Requirements (BRD)
List each requirement with a unique ID in format BR-1, BR-2, etc. For example:
- BR-1: [Requirement description]
- BR-2: [Requirement description]
- BR-3: [Requirement description]
(Continue for all identified requirements)

## 3. Technical Detailed Steps
Numbered list of technical implementation steps. For example:
1. [First step]
2. [Second step]
3. [Third step]
(Continue for all steps)

## 4. Source System Description
Description of source tables, their purpose, and key fields.

## 5. Target System Description
Description of target tables, their purpose, and key fields.

## 6. Data Quality Requirements
- DQ-1: [Data quality requirement]
- DQ-2: [Data quality requirement]

## 7. Error Handling
How errors should be handled, logged, and reported.

Format the output in clear sections with markdown formatting. Be specific and reference actual table and field names from the mapping."""

    return prompt


def create_consolidation_prompt(primary_response: str, 
                                secondary_response: str, 
                                business_context: str = "") -> str:
    """
    Create a prompt to consolidate results from two models.
    
    Args:
        primary_response: Response from primary model
        secondary_response: Response from secondary model
        business_context: Optional business context
        
    Returns:
        Formatted prompt string
    """
    prompt = f"""You are tasked with consolidating two different analyses of the same ETL process.

{f"BUSINESS CONTEXT:{chr(10)}{business_context}{chr(10)}" if business_context else ""}

ANALYSIS 1:
{primary_response}

ANALYSIS 2:
{secondary_response}

Please consolidate these two analyses into a single, comprehensive BRD document that:
1. Combines the best insights from both analyses
2. Resolves any contradictions
3. Uses the BR-1, BR-2 format for Business Requirements
4. Uses numbered steps for Technical Detailed Steps
5. Provides a complete and coherent set of requirements

Format the output in clear sections with markdown formatting."""

    return prompt


def create_lineage_update_prompt(current_lineage_summary: str, 
                                  generated_requirements: str, 
                                  additional_requirements: str) -> str:
    """
    Create a prompt to suggest lineage updates based on requirements.
    
    Args:
        current_lineage_summary: Summary of current lineage
        generated_requirements: Previously generated requirements
        additional_requirements: New requirements to apply
        
    Returns:
        Formatted prompt string
    """
    prompt = f"""You are a Data Engineer expert in ETL processes.

Based on the current lineage and the business requirements, identify any changes or additions needed to the Source-to-Target mapping.

CURRENT LINEAGE:
{current_lineage_summary}

GENERATED REQUIREMENTS:
{generated_requirements}

ADDITIONAL REQUIREMENTS/CHANGES:
{additional_requirements}

Please analyze and provide:
1. **New Fields Needed**: Any new target fields that should be added based on requirements
2. **Modified Mappings**: Any existing mappings that need to be changed
3. **New Transformations**: Any new transformation logic needed
4. **Validation Rules**: Any new validation or business rules to add

For each change, specify:
- Target Field Name
- Source Table
- Source Field  
- Transformation Logic (if any)
- Reason for change

Format as a structured list that can be used to update the lineage."""

    return prompt


def create_sql_generation_prompt(lineage_summary: str,
                                 target_table: str,
                                 sql_type: str = "INSERT",
                                 additional_context: str = "",
                                 target_platform: str = "snowflake") -> str:
    """
    Create a prompt for AI-assisted SQL generation.

    Args:
        lineage_summary: Summary of lineage data
        target_table: Target table name
        sql_type: Type of SQL to generate (INSERT, MERGE, DDL)
        additional_context: Optional additional context
        target_platform: 'snowflake' or 'databricks'

    Returns:
        Formatted prompt string
    """
    platform_label = "Databricks Spark SQL" if target_platform == "databricks" else "Snowflake"

    prompt = f"""You are a {platform_label} expert. Generate optimized {sql_type} SQL based on the following mapping information.

TARGET TABLE: {target_table}

MAPPING INFORMATION:
{lineage_summary}

{f"ADDITIONAL CONTEXT:{chr(10)}{additional_context}{chr(10)}" if additional_context else ""}

Please generate:
1. The {sql_type} SQL statement with proper {platform_label} syntax
2. Any necessary data type conversions
3. Handle NULL values appropriately
4. Include comments explaining complex transformations
5. Follow {platform_label} best practices

Output only valid {platform_label} SQL that can be executed directly."""

    return prompt


def create_expression_conversion_prompt(informatica_expression: str,
                                        target_platform: str = "snowflake") -> str:
    """
    Create a prompt to convert complex Informatica expressions to target SQL.

    Args:
        informatica_expression: Informatica expression to convert
        target_platform: 'snowflake' or 'databricks'

    Returns:
        Formatted prompt string
    """
    platform_label = "Databricks Spark SQL" if target_platform == "databricks" else "Snowflake SQL"

    prompt = f"""You are an expert in both Informatica PowerCenter expressions and {platform_label}.

Convert the following Informatica expression to equivalent {platform_label}:

INFORMATICA EXPRESSION:
{informatica_expression}

Please provide:
1. The equivalent {platform_label} expression
2. Any assumptions made during conversion
3. Notes on any functionality differences between Informatica and {platform_label}

Output the {platform_label} expression only, followed by brief notes if needed."""

    return prompt


def create_business_name_generation_prompt(column_names: list) -> str:
    """
    Create a prompt to generate business-friendly names for columns.
    
    Args:
        column_names: List of technical column names
        
    Returns:
        Formatted prompt string
    """
    columns_text = chr(10).join(column_names)
    
    prompt = f"""Generate business-friendly names for these technical database column names.

TECHNICAL COLUMN NAMES:
{columns_text}

RULES:
1. Convert abbreviations to full words (ACCT->Account, DOC->Document, NO->Number, DT/DATE->Date, AMT->Amount, QTY->Quantity, PCT->Percent, CD/CODE->Code, DESC->Description, IND->Indicator, NBR/NUM->Number)
2. Use Title Case with spaces
3. Keep names concise but clear for business users

Respond with ONLY valid JSON:
{{"column_mappings": {{"TECHNICAL_NAME": "Business Friendly Name"}}}}

JSON ONLY:"""

    return prompt


def create_conversion_report_prompt(context_summary: str,
                                     business_context: str = "") -> str:
    """
    Create a prompt for generating a comprehensive Informatica Workflow Conversion Report.

    Args:
        context_summary: Structured summary of sources, targets, lineage, transformations
        business_context: Optional business context

    Returns:
        Formatted prompt string
    """
    prompt = f"""You are a senior data engineer documenting an Informatica PowerCenter to modern data platform migration.

Generate a comprehensive **Informatica Workflow Conversion Report** in Markdown format.

{f"BUSINESS CONTEXT:{chr(10)}{business_context}{chr(10)}" if business_context else ""}

PARSED ETL METADATA:
{context_summary}

Generate the report with these sections:

## 1. Executive Summary
Brief overview of the ETL pipeline: its purpose, complexity, source-to-target flow, and key statistics (number of mappings, transformations, lookups).

## 2. Workflow Architecture Overview
- Workflow name and scheduling
- Number of sessions and their execution order
- Overall data flow pattern (Source → Staging → Enrichment → Target)

## 3. Stage-by-Stage Breakdown
For each mapping/session, describe:
- Purpose and what it accomplishes
- Source tables read and target tables loaded
- Key transformations applied
- Load strategy (insert, update, upsert)

## 4. Detailed Transformation Logic
- Expression transformations (null handling, type casting, string manipulation)
- Filter conditions and their business purpose
- Aggregator logic (deduplication keys, group-by columns)
- Specific Informatica functions used and their SQL equivalents

## 5. Dimension Lookup Details
For each lookup:
- Lookup table name and purpose
- Join keys / lookup condition
- Whether it uses point-in-time logic (date-range based SCD2)
- Output columns (Current C_ and Historical H_ keys)

## 6. Update Strategy
- How the pipeline determines INSERT vs UPDATE
- Router/Update Strategy transformation logic
- Key columns used for existence checks
- Any pre/post-SQL operations

## 7. Business Rules & Data Quality
- Data quality filters (blank removal, deduplication)
- Default value assignments (COALESCE patterns)
- Type conversion rules
- Referential integrity through dimension lookups

## 8. Recommended dbt Project Structure
Suggest a dbt project layout for this pipeline:
- Bronze (staging) layer: source table mirroring
- Silver (transform) layer: business logic + lookups
- Tests and documentation
- Materialization strategy

## 9. Source Column Mapping Reference
Provide a summary table of key column mappings grouped by type:
- Natural keys
- Dimension foreign keys
- Dates, amounts, codes

Format the output in clear Markdown with headers, tables, code blocks, and bullet points. Be specific — reference actual table names, column names, and transformation details from the metadata provided."""

    return prompt


def create_workflow_tdd_prompt(workflow_summary: str,
                               mapping_summary: str = "",
                               business_context: str = "",
                               additional_requirements: str = "",
                               is_unified: bool = False) -> str:
    """
    Create a prompt for generating a comprehensive TDD from workflow + mapping XMLs.

    Args:
        workflow_summary: Summary of workflow orchestration (from workflow parser)
        mapping_summary: Summary of mapping lineage (from mapping parser, optional)
        business_context: Optional business context
        additional_requirements: Optional additional requirements
        is_unified: True when workflow_summary already contains enriched mapping details

    Returns:
        Formatted prompt string
    """
    if is_unified:
        data_label = "UNIFIED WORKFLOW + MAPPING DATA"
        data_note = ("The workflow summary below is enriched with mapping details for each session — "
                     "source tables, target tables, transformations, expressions, lookups, and load strategies "
                     "are included inline under each session.")
    else:
        data_label = "WORKFLOW ORCHESTRATION"
        data_note = ""

    prompt = f"""You are a Business Analyst and ETL Architect expert in Informatica PowerCenter workflows and data warehousing.

Based on the following INFORMATICA WORKFLOW AND MAPPING INFORMATION, generate a comprehensive Technical Design Document (TDD) that covers both the orchestration (workflow) and data transformation (mapping) aspects.

{f"BUSINESS CONTEXT:{chr(10)}{business_context}{chr(10)}" if business_context else ""}

{f"ADDITIONAL REQUIREMENTS:{chr(10)}{additional_requirements}{chr(10)}" if additional_requirements else ""}

{data_label}:
{data_note}
{workflow_summary}

{f"MAPPING / LINEAGE DETAILS:{chr(10)}{mapping_summary}" if mapping_summary else ""}

Please generate the TDD with the following sections:

## 1. Executive Summary
Brief description of the end-to-end ETL process, its business purpose, and the data flow.

## 2. Workflow Orchestration
- Workflow name, description, and scheduling
- Task execution order and dependencies (DAG)
- Conditional branching logic (success/failure conditions)
- Error handling and recovery strategy
- Email notifications and alerting

## 3. Session Details
For each session in the workflow:
- Session name and the mapping it executes
- The specific source tables/files read by the mapping
- The transformation pipeline (in execution order)
- Key business logic in Expression transforms
- Lookup enrichment details (lookup tables, conditions, SQL overrides)
- Filtering/routing logic
- The target table(s) loaded and the load strategy
- Pre/Post SQL operations
- File configurations (if file-based sources)
- Connection details

## 3.5. End-to-End Data Flow
Trace the data flow across all sessions in execution order. Show how data moves from initial source through intermediate staging to final target, identifying which sessions handle each stage. Present this as a clear pipeline diagram or numbered flow.

## 4. Source System Analysis
Describe each source (tables, files), its purpose, key fields, and connection type.

## 5. Target System Analysis
Describe each target table, its purpose, key fields, and load method.

## 6. Business Functional Requirements
List each requirement with a unique ID in format BR-1, BR-2, etc.
- BR-1: [Requirement]
- BR-2: [Requirement]
(Continue for all requirements)

## 7. Transformation Logic
Describe key transformations, expressions, lookups, routers, filters, aggregators, and their business purpose.

## 8. Data Quality Requirements
- DQ-1: [Requirement]
- DQ-2: [Requirement]

## 9. Error Handling & Recovery
- Error notification mechanism (email addresses, conditions)
- Recovery strategy for each session
- Failure cascading behavior (fail parent workflow?)
- Reject file locations and handling

## 10. Operational Requirements
- Scheduling (on-demand, scheduled, interval)
- Parameter files and their purpose
- Log file locations
- Shell commands / file archival steps
- Concurrency settings

## 11. Technical Notes
Any SQL overrides, filters, special configurations, or platform-specific considerations.

Format the output in clear markdown sections. Be specific and reference actual names from the workflow and mappings."""

    return prompt
