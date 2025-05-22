# DocumentationToMetadata Transformer

A DataHub transformer that extracts key-value pairs from documentation and creates various types of metadata in DataHub, including custom properties, tags, glossary terms, and owners.

## Features

- Extracts key-value pairs from documentation using configurable regex patterns
- Supports multiple metadata types:
  - Custom Properties
  - Tags
  - Glossary Terms
  - Owners (with support for different ownership types)
- Configurable key mappings
- Flexible documentation field parsing
- Support for both PATCH and OVERWRITE semantics

## Installation

### Core/CLI Installation

1. Place the `documentation_to_metadata_transformer.py` file in your DataHub transformers directory
2. Add the transformer to your DataHub ingestion configuration


## Configuration

Here's an example configuration:

```yaml
transformers:
  - type: "DocumentationToMetadata"
    config:
      documentation_field: "description"  # Field containing documentation to parse
      key_value_pattern: "^\s*-\s*([^:]+):\s*(.+?)(?=\n\s*-\s*[^:]+:|$)"  # Default pattern for bullet points
      semantics: "PATCH"  # or "OVERWRITE"
      key_mappings:
        - key_name: "Owner"
          metadata_type: "owner"
          target_name: "DATAOWNER"
          description: "Data owner of the asset"
        - key_name: "Department"
          metadata_type: "custom_property"
          target_name: "department"
          description: "Owning department"
        - key_name: "Classification"
          metadata_type: "tag"
          target_name: "urn:li:tag:classification"
          description: "Data classification tag"
        - key_name: "Domain"
          metadata_type: "glossary_term"
          target_name: "urn:li:glossaryTerm:domain"
          description: "Business domain"
```

### Configuration Options

#### Top-level Configuration

- `documentation_field`: Field name containing the documentation to parse (default: "description")
- `key_value_pattern`: Regex pattern to match key-value pairs (default matches bullet points)
- `semantics`: How to handle existing aspect values ("PATCH" or "OVERWRITE")
- `key_mappings`: List of key mappings to extract from documentation

#### Key Mapping Configuration

Each key mapping requires:
- `key_name`: Name of the key to extract from documentation
- `metadata_type`: Type of metadata to create (one of: "custom_property", "tag", "glossary_term", "owner")
- `target_name`: Name of the target metadata
  - For custom properties: the property name
  - For tags: the tag URN
  - For glossary terms: the term URN
  - For owners: the ownership type (DATAOWNER, STAKEHOLDER, DELEGATE, PRODUCER, CONSUMER, TECHNICAL_OWNER)
- `description`: Optional description of what the key represents

## Usage

### Documentation Format

The transformer expects documentation in a format with key-value pairs. By default, it looks for bullet points with colons:

```markdown
- Owner: John Doe
- Department: Engineering
- Classification: Confidential
- Domain: Customer Data
```

### Example

Given the following documentation:

```markdown
- Owner: Jane Smith
- Department: Data Science
- Classification: Internal
- Domain: Analytics
```

With the configuration above, the transformer will:
1. Create a custom property "department" with value "Data Science"
2. Add a tag "urn:li:tag:classification" with value "Internal"
3. Add a glossary term "urn:li:glossaryTerm:domain" with value "Analytics"
4. Create an owner "Jane Smith" with type DATAOWNER

## Supported Entity Types

The transformer supports the following entity types:
- dataset
- container
- dataFlow
- dataJob
- chart
- dashboard

## Error Handling

The transformer includes comprehensive error handling and logging:
- Invalid ownership types fall back to DATAOWNER
- Failed MCP creation is logged but doesn't stop processing
- Detailed logging of each step in the transformation process

## Logging

The transformer provides detailed logging of its operations:
- Key-value pair extraction
- MCP creation
- Aspect processing
- Error conditions
