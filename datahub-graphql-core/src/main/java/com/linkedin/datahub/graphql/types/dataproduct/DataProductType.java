package com.linkedin.datahub.graphql.types.dataproduct;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.authorization.ConjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.authorization.DisjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.BatchDataProductUpdateInput;
import com.linkedin.datahub.graphql.generated.DataProduct;
import com.linkedin.datahub.graphql.generated.DataProductUpdateInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.BrowseResults;
import com.linkedin.datahub.graphql.generated.BrowsePath;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.BatchMutableType;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import com.linkedin.datahub.graphql.types.dataproduct.mappers.DataProductMapper;
import com.linkedin.datahub.graphql.types.dataproduct.mappers.DataProductUpdateInputMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowsePathsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowseResultMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.DataFetcherResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.datahub.graphql.Constants.BROWSE_PATH_DELIMITER;
import static com.linkedin.metadata.Constants.*;


public class DataProductType implements BrowsableEntityType<DataProduct, String>,
        BatchMutableType<DataProductUpdateInput, BatchDataProductUpdateInput, DataProduct> {

    static final Set<String> ASPECTS_TO_RESOLVE = ImmutableSet.of(
        DATA_PRODUCT_KEY_ASPECT_NAME,
        DATA_PRODUCT_PROPERTIES_ASPECT_NAME,
        DATA_PRODUCT_INPUT_OUTPUT_ASPECT_NAME,
        DOMAINS_ASPECT_NAME,
        CONTAINER_ASPECT_NAME,
        STATUS_ASPECT_NAME,
        GLOSSARY_TERMS_ASPECT_NAME,
        DEPRECATION_ASPECT_NAME,
        GLOBAL_TAGS_ASPECT_NAME
    );

    private static final Set<String> FACET_FIELDS = ImmutableSet.of("origin", "platform");
    private static final String ENTITY_NAME = "dataProduct";

    private final EntityClient _entityClient;

    public DataProductType(final EntityClient entityClient) {
        _entityClient = entityClient;
    }

    @Override
    public Class<DataProduct> objectClass() {
        return DataProduct.class;
    }

    @Override
    public Class<DataProductUpdateInput> inputClass() {
        return DataProductUpdateInput.class;
    }

    @Override
    public Class<BatchDataProductUpdateInput[]> batchInputClass() {
        return BatchDataProductUpdateInput[].class;
    }

    @Override
    public EntityType type() {
        return EntityType.DATA_PRODUCT;
    }

    @Override
    public Function<Entity, String> getKeyProvider() {
        return Entity::getUrn;
    }

    @Override
    public List<DataFetcherResult<DataProduct>> batchLoad(@Nonnull final List<String> urnStrs,
        @Nonnull final QueryContext context) {
        try {
            final List<Urn> urns = urnStrs.stream()
                .map(UrnUtils::getUrn)
                .collect(Collectors.toList());

            final Map<Urn, EntityResponse> dataProductMap =
                _entityClient.batchGetV2(
                    Constants.DATA_PRODUCT_ENTITY_NAME,
                    new HashSet<>(urns),
                    ASPECTS_TO_RESOLVE,
                    context.getAuthentication());

            final List<EntityResponse> gmsResults = new ArrayList<>();
            for (Urn urn : urns) {
                gmsResults.add(dataProductMap.getOrDefault(urn, null));
            }
            return gmsResults.stream()
                .map(gmsDataProduct -> gmsDataProduct == null ? null : DataFetcherResult.<DataProduct>newResult()
                    .data(DataProductMapper.map(gmsDataProduct))
                    .build())
                .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch load DataProducts", e);
        }
    }

    @Override
    public BrowseResults browse(@Nonnull List<String> path,
                                @Nullable List<FacetFilterInput> filters,
                                int start,
                                int count,
                                @Nonnull final QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        final String pathStr = path.size() > 0 ? BROWSE_PATH_DELIMITER + String.join(BROWSE_PATH_DELIMITER, path) : "";
        final BrowseResult result = _entityClient.browse(
                "dataProduct",
                pathStr,
                facetFilters,
                start,
                count,
            context.getAuthentication());
        return BrowseResultMapper.map(result);
    }

    @Override
    public List<BrowsePath> browsePaths(@Nonnull String urn, @Nonnull final QueryContext context) throws Exception {
        final StringArray result = _entityClient.getBrowsePaths(DataProductUtils.getDataProductUrn(urn), context.getAuthentication());
        return BrowsePathsMapper.map(result);
    }

    @Override
    public List<DataProduct> batchUpdate(@Nonnull BatchDataProductUpdateInput[] input, @Nonnull QueryContext context) throws Exception {
        final Urn actor = Urn.createFromString(context.getAuthentication().getActor().toUrnStr());

        final Collection<MetadataChangeProposal> proposals = Arrays.stream(input).map(updateInput -> {
            if (isAuthorized(updateInput.getUrn(), updateInput.getUpdate(), context)) {
                Collection<MetadataChangeProposal> datasetProposals = DataProductUpdateInputMapper.map(updateInput.getUpdate(), actor);
                datasetProposals.forEach(proposal -> proposal.setEntityUrn(UrnUtils.getUrn(updateInput.getUrn())));
                return datasetProposals;
            }
            throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
        }).flatMap(Collection::stream).collect(Collectors.toList());

        final List<String> urns = Arrays.stream(input).map(BatchDataProductUpdateInput::getUrn).collect(Collectors.toList());

        try {
            _entityClient.batchIngestProposals(proposals, context.getAuthentication(), false);
        } catch (RemoteInvocationException e) {
            throw new RuntimeException(String.format("Failed to write entity with urn %s", urns), e);
        }

        return batchLoad(urns, context).stream().map(DataFetcherResult::getData).collect(Collectors.toList());
    }

    @Override
    public DataProduct update(@Nonnull String urn, @Nonnull DataProductUpdateInput input, @Nonnull QueryContext context) throws Exception {
        if (isAuthorized(urn, input, context)) {
            final CorpuserUrn actor = CorpuserUrn.createFromString(context.getAuthentication().getActor().toUrnStr());
            final Collection<MetadataChangeProposal> proposals = DataProductUpdateInputMapper.map(input, actor);
            proposals.forEach(proposal -> proposal.setEntityUrn(UrnUtils.getUrn(urn)));

            try {
                _entityClient.batchIngestProposals(proposals, context.getAuthentication(), false);
            } catch (RemoteInvocationException e) {
                throw new RuntimeException(String.format("Failed to write entity with urn %s", urn), e);
            }

            return load(urn, context).getData();
        }
        throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    }

    private boolean isAuthorized(@Nonnull String urn, @Nonnull DataProductUpdateInput update, @Nonnull QueryContext context) {
        // Decide whether the current principal should be allowed to update the DataProduct.
        final DisjunctivePrivilegeGroup orPrivilegeGroups = getAuthorizedPrivileges(update);
        return AuthorizationUtils.isAuthorized(
            context.getAuthorizer(),
            context.getAuthentication().getActor().toUrnStr(),
            PoliciesConfig.DATASET_PRIVILEGES.getResourceType(),
            urn,
            orPrivilegeGroups);
    }

    private DisjunctivePrivilegeGroup getAuthorizedPrivileges(final DataProductUpdateInput updateInput) {

        final ConjunctivePrivilegeGroup allPrivilegesGroup = new ConjunctivePrivilegeGroup(ImmutableList.of(
            PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()
        ));

        List<String> specificPrivileges = new ArrayList<>();
        if (updateInput.getOwnership() != null) {
            specificPrivileges.add(PoliciesConfig.EDIT_ENTITY_OWNERS_PRIVILEGE.getType());
        }
        if (updateInput.getDeprecation() != null) {
            specificPrivileges.add(PoliciesConfig.EDIT_ENTITY_STATUS_PRIVILEGE.getType());
        }

        final ConjunctivePrivilegeGroup specificPrivilegeGroup = new ConjunctivePrivilegeGroup(specificPrivileges);

        // If you either have all entity privileges, or have the specific privileges required, you are authorized.
        return new DisjunctivePrivilegeGroup(ImmutableList.of(
            allPrivilegesGroup,
            specificPrivilegeGroup
        ));
    }
}
