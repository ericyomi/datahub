package com.linkedin.datahub.graphql.types.dataproduct;

import com.linkedin.common.urn.DatasetUrn;

import java.net.URISyntaxException;

public class DataProductUtils {

    private DataProductUtils() { }

    static DatasetUrn getDataProductUrn(String urnStr) {
        try {
            return DatasetUrn.createFromString(urnStr);
        } catch (URISyntaxException e) {
            throw new RuntimeException(String.format("Failed to retrieve DataProduct with urn %s, invalid urn", urnStr));
        }
    }
}
