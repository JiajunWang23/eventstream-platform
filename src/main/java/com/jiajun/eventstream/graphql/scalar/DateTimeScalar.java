package com.jiajun.eventstream.graphql.scalar;

import graphql.GraphQLContext;
import graphql.execution.CoercedVariables;
import graphql.language.StringValue;
import graphql.language.Value;
import graphql.schema.Coercing;
import graphql.schema.CoercingParseLiteralException;
import graphql.schema.CoercingParseValueException;
import graphql.schema.CoercingSerializeException;
import graphql.schema.GraphQLScalarType;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Locale;

/**
 * ISO-8601 {@code DateTime} scalar backed by {@link Instant}.
 */
public final class DateTimeScalar {

    public static final GraphQLScalarType INSTANCE = GraphQLScalarType.newScalar()
            .name("DateTime")
            .description("ISO-8601 instant")
            .coercing(new Coercing<Instant, String>() {
                @Override
                public String serialize(Object dataFetcherResult, GraphQLContext context, Locale locale) {
                    if (dataFetcherResult instanceof Instant instant) {
                        return instant.toString();
                    }
                    if (dataFetcherResult instanceof OffsetDateTime odt) {
                        return odt.toInstant().toString();
                    }
                    if (dataFetcherResult instanceof ZonedDateTime zdt) {
                        return zdt.toInstant().toString();
                    }
                    if (dataFetcherResult instanceof String s) {
                        return parse(s).toString();
                    }
                    throw new CoercingSerializeException("Cannot serialize " + dataFetcherResult + " as DateTime");
                }

                @Override
                public Instant parseValue(Object input, GraphQLContext context, Locale locale) {
                    if (input instanceof Instant instant) {
                        return instant;
                    }
                    if (input instanceof String s) {
                        try {
                            return parse(s);
                        } catch (DateTimeParseException e) {
                            throw new CoercingParseValueException("Invalid DateTime: " + s, e);
                        }
                    }
                    throw new CoercingParseValueException("Expected an ISO-8601 string, got " + input);
                }

                @Override
                public Instant parseLiteral(Value<?> input, CoercedVariables variables, GraphQLContext context, Locale locale) {
                    if (input instanceof StringValue s) {
                        try {
                            return parse(s.getValue());
                        } catch (DateTimeParseException e) {
                            throw new CoercingParseLiteralException("Invalid DateTime: " + s.getValue(), e);
                        }
                    }
                    throw new CoercingParseLiteralException("Expected a string literal for DateTime");
                }
            })
            .build();

    private DateTimeScalar() {
    }

    static Instant parse(String text) {
        try {
            return Instant.parse(text);
        } catch (DateTimeParseException e) {
            return OffsetDateTime.parse(text).toInstant();
        }
    }
}
