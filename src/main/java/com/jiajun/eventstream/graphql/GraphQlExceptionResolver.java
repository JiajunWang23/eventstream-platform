package com.jiajun.eventstream.graphql;

import com.jiajun.eventstream.exception.ConflictException;
import com.jiajun.eventstream.exception.NotFoundException;
import com.jiajun.eventstream.exception.PublishException;
import graphql.GraphQLError;
import graphql.GraphqlErrorBuilder;
import graphql.schema.DataFetchingEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.graphql.execution.DataFetcherExceptionResolverAdapter;
import org.springframework.graphql.execution.ErrorType;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Maps domain exceptions to GraphQL error classifications instead of the generic INTERNAL_ERROR.
 */
@Component
public class GraphQlExceptionResolver extends DataFetcherExceptionResolverAdapter {

    private static final Logger log = LoggerFactory.getLogger(GraphQlExceptionResolver.class);

    @Override
    protected GraphQLError resolveToSingleError(Throwable ex, DataFetchingEnvironment env) {
        ErrorType type;
        if (ex instanceof NotFoundException) {
            type = ErrorType.NOT_FOUND;
        } else if (ex instanceof IllegalArgumentException) {
            type = ErrorType.BAD_REQUEST;
        } else if (ex instanceof ConflictException) {
            type = ErrorType.BAD_REQUEST;
        } else if (ex instanceof PublishException) {
            log.error("Publish failed for {}", env.getField().getName(), ex);
            type = ErrorType.INTERNAL_ERROR;
        } else {
            return null;
        }
        return GraphqlErrorBuilder.newError(env)
                .errorType(type)
                .message(ex.getMessage())
                .extensions(Map.of("exception", ex.getClass().getSimpleName()))
                .build();
    }
}
