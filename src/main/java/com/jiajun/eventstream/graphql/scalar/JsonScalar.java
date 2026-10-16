package com.jiajun.eventstream.graphql.scalar;

import graphql.GraphQLContext;
import graphql.execution.CoercedVariables;
import graphql.language.ArrayValue;
import graphql.language.BooleanValue;
import graphql.language.EnumValue;
import graphql.language.FloatValue;
import graphql.language.IntValue;
import graphql.language.NullValue;
import graphql.language.ObjectField;
import graphql.language.ObjectValue;
import graphql.language.StringValue;
import graphql.language.Value;
import graphql.language.VariableReference;
import graphql.schema.Coercing;
import graphql.schema.CoercingParseLiteralException;
import graphql.schema.GraphQLScalarType;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * A {@code JSON} scalar that passes arbitrary JSON values through untouched and can
 * parse inline object/array literals from a query document.
 */
public final class JsonScalar {

    public static final GraphQLScalarType INSTANCE = GraphQLScalarType.newScalar()
            .name("JSON")
            .description("Arbitrary JSON value")
            .coercing(new Coercing<Object, Object>() {
                @Override
                public Object serialize(Object dataFetcherResult, GraphQLContext context, Locale locale) {
                    return dataFetcherResult;
                }

                @Override
                public Object parseValue(Object input, GraphQLContext context, Locale locale) {
                    return input;
                }

                @Override
                public Object parseLiteral(Value<?> input, CoercedVariables variables, GraphQLContext context, Locale locale) {
                    return fromLiteral(input, variables);
                }
            })
            .build();

    private JsonScalar() {
    }

    static Object fromLiteral(Value<?> value, CoercedVariables variables) {
        if (value instanceof NullValue) {
            return null;
        }
        if (value instanceof StringValue s) {
            return s.getValue();
        }
        if (value instanceof IntValue i) {
            BigInteger n = i.getValue();
            if (n.bitLength() < 32) {
                return n.intValue();
            }
            return n.bitLength() < 64 ? (Object) n.longValue() : n;
        }
        if (value instanceof FloatValue f) {
            return f.getValue().doubleValue();
        }
        if (value instanceof BooleanValue b) {
            return b.isValue();
        }
        if (value instanceof EnumValue e) {
            return e.getName();
        }
        if (value instanceof VariableReference ref) {
            return variables.get(ref.getName());
        }
        if (value instanceof ArrayValue array) {
            List<Object> list = new ArrayList<>();
            for (Value<?> element : array.getValues()) {
                list.add(fromLiteral(element, variables));
            }
            return list;
        }
        if (value instanceof ObjectValue object) {
            Map<String, Object> map = new LinkedHashMap<>();
            for (ObjectField field : object.getObjectFields()) {
                map.put(field.getName(), fromLiteral(field.getValue(), variables));
            }
            return map;
        }
        throw new CoercingParseLiteralException("Unsupported JSON literal: " + value.getClass().getSimpleName());
    }
}
