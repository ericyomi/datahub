
package com.linkedin.datahub.graphql.types.dataproduct;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.Status;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataProduct;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DataProductKey;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.DataFetcherResult;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.*;

public class DataProductTypeTest {

  private static final String TEST_DATA_PRODUCT_1_URN = "urn:li:dataProduct:(urn:dataPlatform:f135DaaP, myDP1, PROD)";
  private static final DataProductKey TEST_DATA_PRODUCT_1_KEY = new DataProductKey()
      .setDataProductId("myDP1");
  private static final DataProductProperties TEST_DATA_PRODUCT_1_PROPERTIES = new DataProductProperties()
      .setDescription("test description")
      .setName("Test Container");
  private static final Ownership TEST_DATA_PRODUCT_1_OWNERSHIP = new Ownership()
      .setOwners(
          new OwnerArray(ImmutableList.of(
              new Owner()
                  .setType(OwnershipType.DATA_STEWARD)
                  .setOwner(Urn.createFromTuple("corpuser", "test")))));
  private static final Status TEST_DATA_PRODUCT_1_STATUS = new Status()
      .setRemoved(false);
  private static final GlobalTags TEST_DATA_PRODUCT_1_TAGS = new GlobalTags()
      .setTags(new TagAssociationArray(ImmutableList.of(new TagAssociation().setTag(new TagUrn("test")))));
  private static final GlossaryTerms TEST_DATA_PRODUCT_1_GLOSSARY_TERMS = new GlossaryTerms()
      .setTerms(new GlossaryTermAssociationArray(ImmutableList.of(new GlossaryTermAssociation().setUrn(new GlossaryTermUrn("term")))));
  private static final com.linkedin.container.Container TEST_DATA_PRODUCT_1_CONTAINER = new com.linkedin.container.Container()
      .setContainer(Urn.createFromTuple(Constants.CONTAINER_ENTITY_NAME, "parent-container"));

  private static final String TEST_DATA_PRODUCT_2_URN = "urn:li:container:guid-2";

  @Test
  public void testBatchLoad() throws Exception {

    EntityClient client = Mockito.mock(EntityClient.class);

    Urn dataProductUrn1 = Urn.createFromString(TEST_DATA_PRODUCT_1_URN);
    Urn dataProductUrn2 = Urn.createFromString(TEST_DATA_PRODUCT_2_URN);

    Map<String, EnvelopedAspect> dataProduct1Aspects = new HashMap<>();
    dataProduct1Aspects.put(
        Constants.CONTAINER_KEY_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_DATA_PRODUCT_1_KEY.data()))
    );
    dataProduct1Aspects.put(
        Constants.CONTAINER_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_DATA_PRODUCT_1_PROPERTIES.data()))
    );
    dataProduct1Aspects.put(
        Constants.OWNERSHIP_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_DATA_PRODUCT_1_OWNERSHIP.data()))
    );
    dataProduct1Aspects.put(
        Constants.STATUS_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_DATA_PRODUCT_1_STATUS.data()))
    );
    dataProduct1Aspects.put(
        Constants.GLOBAL_TAGS_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_DATA_PRODUCT_1_TAGS.data()))
    );
    dataProduct1Aspects.put(
        Constants.GLOSSARY_TERMS_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_DATA_PRODUCT_1_GLOSSARY_TERMS.data()))
    );
    dataProduct1Aspects.put(
        Constants.CONTAINER_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_DATA_PRODUCT_1_CONTAINER.data()))
    );
    dataProduct1Aspects.put(
        Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_DATA_PRODUCT_1_PROPERTIES.data()))
    );
    Mockito.when(client.batchGetV2(
        Mockito.eq(Constants.DATA_PRODUCT_ENTITY_NAME),
        Mockito.eq(new HashSet<>(ImmutableSet.of(dataProductUrn1, dataProductUrn2))),
        Mockito.eq(DataProductType.ASPECTS_TO_RESOLVE),
        Mockito.any(Authentication.class)))
        .thenReturn(ImmutableMap.of(
            dataProductUrn1,
            new EntityResponse()
                .setEntityName(Constants.DATA_PRODUCT_ENTITY_NAME)
                .setUrn(dataProductUrn1)
                .setAspects(new EnvelopedAspectMap(dataProduct1Aspects))));

    DataProductType type = new DataProductType(client);

    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    List<DataFetcherResult<DataProduct>> result = type.batchLoad(ImmutableList.of(TEST_DATA_PRODUCT_1_URN, TEST_DATA_PRODUCT_2_URN), mockContext);

    // Verify response
    Mockito.verify(client, Mockito.times(1)).batchGetV2(
        Mockito.eq(Constants.DATA_PRODUCT_ENTITY_NAME),
        Mockito.eq(ImmutableSet.of(dataProductUrn1, dataProductUrn2)),
        Mockito.eq(DataProductType.ASPECTS_TO_RESOLVE),
        Mockito.any(Authentication.class)
    );

    assertEquals(result.size(), 2);

    System.out.println(result.get(0).getData());

    DataProduct dataProduct1 = result.get(0).getData();
    assertEquals(dataProduct1.getUrn(), TEST_DATA_PRODUCT_1_URN);
    assertEquals(dataProduct1.getType(), EntityType.DATA_PRODUCT);
    assertEquals(dataProduct1.getOwnership().getOwners().size(), 1);
    assertEquals(dataProduct1.getProperties().getDescription(), "test description");
    assertEquals(dataProduct1.getProperties().getName(), "Test Container");
    assertEquals(
        dataProduct1.getGlossaryTerms().getTerms().get(0).getTerm().getUrn(),
        TEST_DATA_PRODUCT_1_GLOSSARY_TERMS.getTerms().get(0).getUrn().toString());
    assertEquals(
        dataProduct1.getTags().getTags().get(0).getTag().getUrn(),
        TEST_DATA_PRODUCT_1_TAGS.getTags().get(0).getTag().toString());
    assertEquals(
        dataProduct1.getContainer().getUrn(),
        TEST_DATA_PRODUCT_1_CONTAINER.getContainer().toString());

    // Assert second element is null.
    assertNull(result.get(1));
  }

  @Test
  public void testBatchLoadClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class).when(mockClient).batchGetV2(
        Mockito.anyString(),
        Mockito.anySet(),
        Mockito.anySet(),
        Mockito.any(Authentication.class));
    DataProductType type = new DataProductType(mockClient);

    // Execute Batch load
    QueryContext context = Mockito.mock(QueryContext.class);
    Mockito.when(context.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    assertThrows(RuntimeException.class, () -> type.batchLoad(ImmutableList.of(TEST_DATA_PRODUCT_1_URN, TEST_DATA_PRODUCT_2_URN),
        context));
  }
}