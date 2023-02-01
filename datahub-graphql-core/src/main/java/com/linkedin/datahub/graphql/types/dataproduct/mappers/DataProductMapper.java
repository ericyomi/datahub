package com.linkedin.datahub.graphql.types.dataproduct.mappers;

import com.linkedin.common.Deprecation;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.generated.DataProduct;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.Container;
import com.linkedin.datahub.graphql.types.common.mappers.DeprecationMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.CustomPropertiesMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.domain.DomainAssociationMapper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.DataProductKey;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.Constants.*;

/**
 * Maps GMS response objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
@Slf4j
public class DataProductMapper implements ModelMapper<EntityResponse, DataProduct> {

    public static final DataProductMapper INSTANCE = new DataProductMapper();

    public static DataProduct map(@Nonnull final EntityResponse dataProduct) {
        return INSTANCE.apply(dataProduct);
    }

    public DataProduct apply(@Nonnull final EntityResponse entityResponse) {
        DataProduct result = new DataProduct();
        Urn entityUrn = entityResponse.getUrn();
        result.setUrn(entityResponse.getUrn().toString());
        result.setType(EntityType.DATA_PRODUCT);

        EnvelopedAspectMap aspectMap = entityResponse.getAspects();

        MappingHelper<DataProduct> mappingHelper = new MappingHelper<>(aspectMap, result);
        mappingHelper.mapToResult(DATA_PRODUCT_KEY_ASPECT_NAME, this::mapDataProductKey);
        mappingHelper.mapToResult(DATA_PRODUCT_PROPERTIES_ASPECT_NAME, (dataProduct, dataMap) ->
                this.mapDataProductProperties(dataProduct, dataMap, entityUrn));
        mappingHelper.mapToResult(OWNERSHIP_ASPECT_NAME, (dataProduct, dataMap) ->
            dataProduct.setOwnership(OwnershipMapper.map(new Ownership(dataMap), entityUrn)));
        mappingHelper.mapToResult(STATUS_ASPECT_NAME, (dataset, dataMap) ->
            dataset.setStatus(StatusMapper.map(new Status(dataMap))));
        mappingHelper.mapToResult(GLOSSARY_TERMS_ASPECT_NAME, (dataset, dataMap) ->
            dataset.setGlossaryTerms(GlossaryTermsMapper.map(new GlossaryTerms(dataMap), entityUrn)));
        mappingHelper.mapToResult(GLOBAL_TAGS_ASPECT_NAME, (dataProduct, dataMap) ->
                this.mapGlobalTags(dataProduct, dataMap, entityUrn));
        mappingHelper.mapToResult(CONTAINER_ASPECT_NAME, this::mapContainers);
        mappingHelper.mapToResult(DOMAINS_ASPECT_NAME, this::mapDomains);
        mappingHelper.mapToResult(DEPRECATION_ASPECT_NAME, (dataset, dataMap) ->
            dataset.setDeprecation(DeprecationMapper.map(new Deprecation(dataMap))));
        return mappingHelper.getResult();
    }

    private void mapDataProductKey(@Nonnull DataProduct dataProduct, @Nonnull DataMap dataMap) {
        final DataProductKey gmsKey = new DataProductKey(dataMap);
        dataProduct.setPlatform(DataPlatform.builder()
            .setType(EntityType.DATA_PLATFORM)
            .setUrn(gmsKey.getPlatform().toString()).build());
    }

    private void mapDataProductProperties(@Nonnull DataProduct dataProduct, @Nonnull DataMap dataMap, @Nonnull Urn entityUrn) {
        final DataProductProperties gmsProperties = new DataProductProperties(dataMap);
        final com.linkedin.datahub.graphql.generated.DataProductProperties properties =
            new com.linkedin.datahub.graphql.generated.DataProductProperties();
        properties.setDescription(gmsProperties.getDescription());
        properties.setCustomProperties(CustomPropertiesMapper.map(gmsProperties.getCustomProperties(), entityUrn));
        gmsProperties.getName();
        properties.setName(gmsProperties.getName());
        dataProduct.setProperties(properties);
    }

    private void mapGlobalTags(@Nonnull DataProduct dataProduct, @Nonnull DataMap dataMap, @Nonnull final Urn entityUrn) {
        com.linkedin.datahub.graphql.generated.GlobalTags globalTags = GlobalTagsMapper.map(new com.linkedin.common.GlobalTags(dataMap), entityUrn);
        dataProduct.setTags(globalTags);
    }

    private void mapContainers(@Nonnull DataProduct dataset, @Nonnull DataMap dataMap) {
        final com.linkedin.container.Container gmsContainer = new com.linkedin.container.Container(dataMap);
        dataset.setContainer(Container
            .builder()
            .setType(EntityType.CONTAINER)
            .setUrn(gmsContainer.getContainer().toString())
            .build());
    }

    private void mapDomains(@Nonnull DataProduct dataset, @Nonnull DataMap dataMap) {
        final Domains domains = new Domains(dataMap);
        dataset.setDomain(DomainAssociationMapper.map(domains, dataset.getUrn()));
    }
}
