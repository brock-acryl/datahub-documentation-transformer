"""
DocumentationToMetadata Transformer for DataHub

This transformer parses documentation for key-value pairs and creates various types of metadata in DataHub:
- Custom Properties
- Tags
- Glossary Terms
- Owners
"""

import json
import traceback
import sys
import copy
import re
import logging
from typing import Dict, List, Optional, Any, Sequence, Union, cast, Set
from enum import Enum

from datahub.configuration.common import ConfigModel, TransformerSemantics
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.transformer.base_transformer import BaseTransformer
from datahub.emitter.mcp import MetadataChangeProposalWrapper, MetadataChangeProposalClass
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    TagAssociationClass,
    GlossaryTermAssociationClass,
    OwnershipClass,
    OwnershipTypeClass,
    CorpUserInfoClass,
    CorpUserEditableInfoClass,
    CorpUserKeyClass,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit

logger = logging.getLogger(__name__)


class MetadataType(str, Enum):
    """Types of metadata that can be created."""
    CUSTOM_PROPERTY = "custom_property"
    TAG = "tag"
    GLOSSARY_TERM = "glossary_term"
    OWNER = "owner"


class DocumentationKeyConfig(ConfigModel):
    """Configuration for a single documentation key to extract."""

    key_name: str
    """Name of the key to extract from documentation."""

    metadata_type: MetadataType
    """Type of metadata to create."""

    target_name: str
    """Name of the target metadata (custom property name, tag urn, glossary term urn, or owner type)."""

    description: Optional[str] = None
    """Optional description of what this key represents."""


class DocumentationToMetadataConfig(ConfigModel):
    """Configuration for the DocumentationToMetadata transformer."""

    key_mappings: List[DocumentationKeyConfig] = []
    """List of key mappings to extract from documentation."""

    documentation_field: str = "description"
    """Field name containing the documentation to parse."""

    key_value_pattern: str = r"^\s*-\s*([^:]+):\s*(.+?)(?=\n\s*-\s*[^:]+:|$)"
    """Regex pattern to match key-value pairs in documentation. Default matches bullet points."""

    semantics: TransformerSemantics = TransformerSemantics.OVERWRITE
    """How to handle existing aspect values."""


class DocumentationToMetadata(BaseTransformer):
    """Transformer that extracts key-value pairs from documentation and creates various types of metadata."""

    ctx: PipelineContext
    config: DocumentationToMetadataConfig
    processed_entities: Dict[str, Any]
    aspect_counter: Dict[str, int]  # Track aspects seen by type
    transform_called: bool = False
    transform_one_called: bool = False
    transform_aspect_called: bool = False
    wu_counter: int = 0
    metadata_mcps: List[MetadataChangeProposalWrapper]

    def __init__(self, config: DocumentationToMetadataConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config
        self.processed_entities = {}
        self.aspect_counter = {}
        self.metadata_mcps = []

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "DocumentationToMetadata":
        config = DocumentationToMetadataConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def entity_types(self) -> List[str]:
        """Return the entity types this transformer applies to."""
        # Support all entity types that might have documentation
        entity_types = ["dataset", "container", "dataFlow", "dataJob", "chart", "dashboard"]
        return entity_types

    def get_aspects_to_transform(self) -> List[str]:
        """Return the aspects this transformer operates on."""
        aspects = [
            # Properties and snapshots
            "datasetProperties",
            "dashboardProperties",
            "chartProperties",
            "containerProperties",
            "dataFlowProperties",
            "dataJobProperties",
            "datasetSnapshot",
            "dashboardSnapshot",
            "chartSnapshot",
            "containerSnapshot",
            "dataFlowSnapshot",
            "dataJobSnapshot",
            # Additional aspects for different metadata types
            "globalTags",
            "glossaryTerms",
            "ownership",
        ]
        return aspects

    def _extract_key_value_pairs(self, documentation: str) -> Dict[str, str]:
        """Extract key-value pairs from documentation using the configured pattern."""
        if not documentation:
            return {}

        # Find all matches in the documentation
        matches = re.finditer(self.config.key_value_pattern, documentation, re.MULTILINE | re.DOTALL)
        
        # Convert matches to dictionary
        key_values = {}
        for match in matches:
            key = match.group(1).strip()
            value = match.group(2).strip()
            # Clean up any remaining whitespace and newlines in the value
            value = re.sub(r'\s+', ' ', value)
            key_values[key] = value

        return key_values

    def _create_metadata_mcp(self, entity_urn: str, key: str, value: str, mapping: DocumentationKeyConfig) -> List[MetadataChangeProposalWrapper]:
        """Create a metadata change proposal based on the mapping type."""
        logger.info(f"Creating MCP for {key}={value} with type {mapping.metadata_type}")
        mcps = []
        
        try:
            if mapping.metadata_type == MetadataType.CUSTOM_PROPERTY:
                # Create custom property
                if not hasattr(self, 'custom_properties'):
                    self.custom_properties = {}
                self.custom_properties[mapping.target_name] = value
                logger.info(f"Added custom property {mapping.target_name}={value}")
                return mcps  # Custom properties are handled in the aspect

            elif mapping.metadata_type == MetadataType.TAG:
                # Create tag association
                mcp = MetadataChangeProposalWrapper(
                    entityUrn=entity_urn,
                    aspect=TagAssociationClass(tag=mapping.target_name)
                )
                logger.info(f"Created tag MCP for {mapping.target_name}")
                mcps.append(mcp)

            elif mapping.metadata_type == MetadataType.GLOSSARY_TERM:
                # Create glossary term association
                mcp = MetadataChangeProposalWrapper(
                    entityUrn=entity_urn,
                    aspect=GlossaryTermAssociationClass(term=mapping.target_name)
                )
                logger.info(f"Created glossary term MCP for {mapping.target_name}")
                mcps.append(mcp)

            elif mapping.metadata_type == MetadataType.OWNER:
                # Create owner entity if it doesn't exist
                owner_urn = f"urn:li:corpuser:{value.lower().replace(' ', '_')}"
                logger.info(f"Creating owner entity with URN: {owner_urn}")
                
                # Create CorpUserInfo
                user_info_mcp = MetadataChangeProposalWrapper(
                    entityUrn=owner_urn,
                    aspect=CorpUserInfoClass(
                        active=True,
                        displayName=value,
                        email=f"{value.lower().replace(' ', '_')}@example.com",  # You might want to make this configurable
                        title="Data Owner",  # You might want to make this configurable
                    )
                )
                logger.info(f"Created CorpUserInfo MCP for {value}")
                mcps.append(user_info_mcp)

                # Create CorpUserEditableInfo
                editable_info_mcp = MetadataChangeProposalWrapper(
                    entityUrn=owner_urn,
                    aspect=CorpUserEditableInfoClass(
                        displayName=value,
                        title="Data Owner",  # You might want to make this configurable
                    )
                )
                logger.info(f"Created CorpUserEditableInfo MCP for {value}")
                mcps.append(editable_info_mcp)

                # Create owner association
                try:
                    # Map the target name to the correct ownership type
                    ownership_type_map = {
                        "DATAOWNER": OwnershipTypeClass.DATAOWNER,
                        "STAKEHOLDER": OwnershipTypeClass.STAKEHOLDER,
                        "DELEGATE": OwnershipTypeClass.DELEGATE,
                        "PRODUCER": OwnershipTypeClass.PRODUCER,
                        "CONSUMER": OwnershipTypeClass.CONSUMER,
                        "TECHNICAL_OWNER": OwnershipTypeClass.TECHNICAL_OWNER
                    }
                    
                    if mapping.target_name in ownership_type_map:
                        ownership_type = ownership_type_map[mapping.target_name]
                    else:
                        logger.error(f"Invalid ownership type: {mapping.target_name}. Valid types are: {', '.join(ownership_type_map.keys())}")
                        ownership_type = OwnershipTypeClass.DATAOWNER
                        logger.info(f"Using fallback ownership type: DATAOWNER")

                    owner_mcp = MetadataChangeProposalWrapper(
                        entityUrn=entity_urn,
                        aspect=OwnershipClass(
                            owners=[{
                                "owner": owner_urn,
                                "type": ownership_type
                            }]
                        )
                    )
                    logger.info(f"Created owner association MCP for {value} with type {mapping.target_name}")
                    mcps.append(owner_mcp)

                except Exception as e:
                    logger.error(f"Error creating owner association: {str(e)}")
                    logger.error(traceback.format_exc())

            logger.info(f"Created {len(mcps)} MCPs for {key}={value}")
            return mcps

        except Exception as e:
            logger.error(f"Error creating MCP for {key}={value}: {str(e)}")
            logger.error(traceback.format_exc())
            return mcps

    def _process_documentation(self, documentation: str) -> Dict[str, str]:
        """Process documentation and extract configured key-value pairs."""
        if not documentation:
            return {}

        # Extract all key-value pairs
        all_pairs = self._extract_key_value_pairs(documentation)
        logger.info(f"Extracted key-value pairs: {all_pairs}")
        
        # Filter to only include configured keys
        result = {}
        for mapping in self.config.key_mappings:
            logger.info(f"Processing mapping: {mapping.key_name} -> {mapping.metadata_type}:{mapping.target_name}")
            if mapping.key_name in all_pairs:
                result[mapping.key_name] = all_pairs[mapping.key_name]
                logger.info(f"Found match for {mapping.key_name}: {all_pairs[mapping.key_name]}")

        logger.info(f"Final processed key-value pairs: {result}")
        return result

    def transform(self, workunits):
        """Transform a list of workunits."""
        self.transform_called = True

        if not isinstance(workunits, list):
            # Handle case where a single workunit is passed
            return self._process_record_envelope(workunits)

        # Process each workunit in the list
        all_results = []
        for envelope in workunits:
            self.wu_counter += 1
            result_workunits = self._process_record_envelope(envelope)
            if isinstance(result_workunits, list):
                all_results.extend(result_workunits)
            else:
                all_results.append(result_workunits)

        return all_results

    def _process_record_envelope(self, envelope):
        """Process a single record envelope containing a workunit."""
        result_envelopes = [envelope]  # Always include the original

        try:
            # Find the workunit in the envelope
            if hasattr(envelope, 'record'):
                workunit = envelope.record
            else:
                return result_envelopes

            # Get metadata from original envelope
            if hasattr(envelope, 'metadata'):
                metadata = envelope.metadata
            else:
                return result_envelopes

            # Extract the URN and any relevant aspects
            urn = None
            properties = None

            # Handle MetadataChangeEventClass (MCE)
            if hasattr(workunit, 'proposedSnapshot'):
                if workunit.proposedSnapshot:
                    urn = workunit.proposedSnapshot.urn

                    if hasattr(workunit.proposedSnapshot, 'aspects'):
                        # Look for properties with documentation
                        for aspect in workunit.proposedSnapshot.aspects:
                            if hasattr(aspect, self.config.documentation_field):
                                properties = aspect

            # Handle MetadataChangeProposalWrapper (MCP)
            elif hasattr(workunit, 'entityUrn') and hasattr(workunit, 'aspect'):
                urn = workunit.entityUrn
                if hasattr(workunit.aspect, self.config.documentation_field):
                    properties = workunit.aspect

            # Process documentation if we have a valid URN and properties
            if urn and properties:
                # Get documentation
                documentation = getattr(properties, self.config.documentation_field, None)
                
                if documentation:
                    # Extract key-value pairs
                    key_values = self._process_documentation(documentation)
                    
                    if key_values:
                        # Initialize customProperties if needed
                        if not hasattr(properties, 'customProperties'):
                            properties.customProperties = {}
                        elif properties.customProperties is None:
                            properties.customProperties = {}

                        # Process each key-value pair
                        for key, value in key_values.items():
                            # Find the mapping for this key
                            mapping = next((m for m in self.config.key_mappings if m.key_name == key), None)
                            if mapping:
                                logger.info(f"Processing key-value pair: {key}={value} with mapping {mapping.metadata_type}:{mapping.target_name}")
                                
                                if mapping.metadata_type == MetadataType.CUSTOM_PROPERTY:
                                    # Handle custom properties
                                    if self.config.semantics == TransformerSemantics.PATCH:
                                        if mapping.target_name not in properties.customProperties:
                                            properties.customProperties[mapping.target_name] = value
                                            logger.info(f"Added custom property {mapping.target_name}={value}")
                                    else:
                                        properties.customProperties[mapping.target_name] = value
                                        logger.info(f"Set custom property {mapping.target_name}={value}")
                                else:
                                    # Create MCPs for other metadata types
                                    mcps = self._create_metadata_mcp(urn, key, value, mapping)
                                    for mcp in mcps:
                                        try:
                                            new_envelope = RecordEnvelope(record=mcp, metadata=metadata)
                                            result_envelopes.append(new_envelope)
                                            self.metadata_mcps.append(mcp)
                                            logger.info(f"Created MCP for {key}={value} with type {mapping.metadata_type}")
                                        except Exception as e:
                                            logger.error(f"Failed to create MCP for {key}={value}: {str(e)}")
                                            logger.error(traceback.format_exc())

                        # Save for reporting
                        self.processed_entities[urn] = {
                            "metadata": key_values,
                            "source": type(properties).__name__
                        }

        except Exception as e:
            logger.error(f"Error processing record envelope: {str(e)}")
            logger.error(traceback.format_exc())

        return result_envelopes

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Any]
    ) -> Optional[Any]:
        """Transform an individual aspect."""
        self.transform_aspect_called = True

        # Keep track of aspects we've seen
        self.aspect_counter[aspect_name] = self.aspect_counter.get(aspect_name, 0) + 1

        try:
            # Check if aspect has documentation
            if aspect and hasattr(aspect, self.config.documentation_field):
                documentation = getattr(aspect, self.config.documentation_field)
                
                if documentation:
                    # Extract key-value pairs
                    key_values = self._process_documentation(documentation)
                    
                    if key_values:
                        # Initialize customProperties if needed
                        if not hasattr(aspect, 'customProperties'):
                            aspect.customProperties = {}
                        elif aspect.customProperties is None:
                            aspect.customProperties = {}

                        # Process each key-value pair
                        for key, value in key_values.items():
                            # Find the mapping for this key
                            mapping = next((m for m in self.config.key_mappings if m.key_name == key), None)
                            if mapping:
                                if mapping.metadata_type == MetadataType.CUSTOM_PROPERTY:
                                    # Handle custom properties
                                    if self.config.semantics == TransformerSemantics.PATCH:
                                        if mapping.target_name not in aspect.customProperties:
                                            aspect.customProperties[mapping.target_name] = value
                                    else:
                                        aspect.customProperties[mapping.target_name] = value
                                else:
                                    # Create MCP for other metadata types
                                    mcp = self._create_metadata_mcp(entity_urn, key, value, mapping)
                                    if mcp:
                                        self.metadata_mcps.append(mcp)

                        # Save for reporting
                        self.processed_entities[entity_urn] = {
                            "metadata": key_values,
                            "source": aspect_name
                        }

            # Process Snapshot aspects specially
            elif aspect_name.endswith("Snapshot") and hasattr(aspect, 'aspects'):
                for sub_aspect in aspect.aspects:
                    if hasattr(sub_aspect, self.config.documentation_field):
                        documentation = getattr(sub_aspect, self.config.documentation_field)
                        
                        if documentation:
                            # Extract key-value pairs
                            key_values = self._process_documentation(documentation)
                            
                            if key_values:
                                # Initialize customProperties if needed
                                if not hasattr(sub_aspect, 'customProperties'):
                                    sub_aspect.customProperties = {}
                                elif sub_aspect.customProperties is None:
                                    sub_aspect.customProperties = {}

                                # Process each key-value pair
                                for key, value in key_values.items():
                                    # Find the mapping for this key
                                    mapping = next((m for m in self.config.key_mappings if m.key_name == key), None)
                                    if mapping:
                                        if mapping.metadata_type == MetadataType.CUSTOM_PROPERTY:
                                            # Handle custom properties
                                            if self.config.semantics == TransformerSemantics.PATCH:
                                                if mapping.target_name not in sub_aspect.customProperties:
                                                    sub_aspect.customProperties[mapping.target_name] = value
                                            else:
                                                sub_aspect.customProperties[mapping.target_name] = value
                                        else:
                                            # Create MCP for other metadata types
                                            mcp = self._create_metadata_mcp(entity_urn, key, value, mapping)
                                            if mcp:
                                                self.metadata_mcps.append(mcp)

                                # Save for reporting
                                self.processed_entities[entity_urn] = {
                                    "metadata": key_values,
                                    "source": f"{aspect_name}.{type(sub_aspect).__name__}"
                                }

        except Exception:
            pass

        return aspect

    def handle_end_of_stream(
        self,
    ) -> Sequence[Union[MetadataChangeProposalWrapper, MetadataChangeProposalClass]]:
        """Return any additional workunits created during transformation."""
        return self.metadata_mcps 