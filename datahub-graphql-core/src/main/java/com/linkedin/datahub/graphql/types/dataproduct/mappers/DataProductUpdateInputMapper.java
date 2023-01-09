package com.linkedin.datahub.graphql.types.dataproduct.mappers;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.generated.DataProductUpdateInput;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipUpdateMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.UpdateMappingHelper;
import com.linkedin.datahub.graphql.types.mappers.InputModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.TagAssociationUpdateMapper;
import com.linkedin.dataset.DatasetDeprecation;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.EditableSchemaFieldInfo;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;

import static com.linkedin.metadata.Constants.*;


public class DataProductUpdateInputMapper implements InputModelMapper<DataProductUpdateInput, Collection<MetadataChangeProposal>, Urn> {

  public static final DataProductUpdateInputMapper INSTANCE = new DataProductUpdateInputMapper();

  public static Collection<MetadataChangeProposal> map(
      @Nonnull final DataProductUpdateInput dataProductUpdateInput,
      @Nonnull final Urn actor) {
    return INSTANCE.apply(dataProductUpdateInput, actor);
  }

  @Override
  public Collection<MetadataChangeProposal> apply(
      @Nonnull final DataProductUpdateInput dataProductUpdateInput,
      @Nonnull final Urn actor) {
    final Collection<MetadataChangeProposal> proposals = new ArrayList<>(6);
    final UpdateMappingHelper updateMappingHelper = new UpdateMappingHelper(DATA_PRODUCT_ENTITY_NAME);
    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actor, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    if (dataProductUpdateInput.getOwnership() != null) {
      proposals.add(updateMappingHelper.aspectToProposal(
          OwnershipUpdateMapper.map(dataProductUpdateInput.getOwnership(), actor), OWNERSHIP_ASPECT_NAME));
    }

    if (dataProductUpdateInput.getDeprecation() != null) {
      final DatasetDeprecation deprecation = new DatasetDeprecation();
      deprecation.setDeprecated(dataProductUpdateInput.getDeprecation().getDeprecated());
      if (dataProductUpdateInput.getDeprecation().getDecommissionTime() != null) {
        deprecation.setDecommissionTime(dataProductUpdateInput.getDeprecation().getDecommissionTime());
      }
      deprecation.setNote(dataProductUpdateInput.getDeprecation().getNote());
      deprecation.setActor(actor, SetMode.IGNORE_NULL);
      proposals.add(updateMappingHelper.aspectToProposal(deprecation, DATASET_DEPRECATION_ASPECT_NAME));
    }

    if (dataProductUpdateInput.getTags() != null) {
      final GlobalTags globalTags = new GlobalTags();
      globalTags.setTags(new TagAssociationArray(dataProductUpdateInput.getTags()
              .getTags()
              .stream()
              .map(element -> TagAssociationUpdateMapper.map(element))
              .collect(Collectors.toList())));
      proposals.add(updateMappingHelper.aspectToProposal(globalTags, GLOBAL_TAGS_ASPECT_NAME));
    }

    return proposals;
  }

  private EditableSchemaFieldInfo mapSchemaFieldInfo(
      final com.linkedin.datahub.graphql.generated.EditableSchemaFieldInfoUpdate schemaFieldInfo
  ) {
    final EditableSchemaFieldInfo output = new EditableSchemaFieldInfo();

    if (schemaFieldInfo.getDescription() != null) {
      output.setDescription(schemaFieldInfo.getDescription());
    }
    output.setFieldPath(schemaFieldInfo.getFieldPath());

    if (schemaFieldInfo.getGlobalTags() != null) {
      final GlobalTags globalTags = new GlobalTags();
      globalTags.setTags(new TagAssociationArray(schemaFieldInfo.getGlobalTags().getTags().stream().map(
          element -> TagAssociationUpdateMapper.map(element)).collect(Collectors.toList())));
      output.setGlobalTags(globalTags);
    }

    return output;
  }
}