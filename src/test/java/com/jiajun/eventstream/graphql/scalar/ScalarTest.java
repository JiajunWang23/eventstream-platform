package com.jiajun.eventstream.graphql.scalar;

import graphql.GraphQLContext;
import graphql.execution.CoercedVariables;
import graphql.language.ArrayValue;
import graphql.language.BooleanValue;
import graphql.language.IntValue;
import graphql.language.ObjectField;
import graphql.language.ObjectValue;
import graphql.language.StringValue;
import graphql.language.VariableReference;
import graphql.schema.CoercingParseValueException;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ScalarTest {

    @Test
    void jsonScalarParsesNestedObjectLiterals() {
        ObjectValue literal = ObjectValue.newObjectValue()
                .objectField(ObjectField.newObjectField().name("orderId").value(IntValue.newIntValue(BigInteger.valueOf(42)).build()).build())
                .objectField(ObjectField.newObjectField().name("tags").value(ArrayValue.newArrayValue()
                        .value(StringValue.newStringValue("vip").build())
                        .value(BooleanValue.newBooleanValue(true).build())
                        .build()).build())
                .objectField(ObjectField.newObjectField().name("ref").value(VariableReference.newVariableReference().name("who").build()).build())
                .build();
        CoercedVariables variables = CoercedVariables.of(Map.of("who", "alice"));

        Object parsed = JsonScalar.INSTANCE.getCoercing()
                .parseLiteral(literal, variables, GraphQLContext.getDefault(), Locale.ROOT);

        assertThat(parsed).isInstanceOf(Map.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) parsed;
        assertThat(map).containsEntry("orderId", 42);
        assertThat(map).containsEntry("tags", List.of("vip", true));
        assertThat(map).containsEntry("ref", "alice");
    }

    @Test
    void jsonScalarPassesValuesThrough() {
        Map<String, Object> value = Map.of("a", 1);
        assertThat(JsonScalar.INSTANCE.getCoercing().serialize(value, GraphQLContext.getDefault(), Locale.ROOT)).isSameAs(value);
        assertThat(JsonScalar.INSTANCE.getCoercing().parseValue(value, GraphQLContext.getDefault(), Locale.ROOT)).isSameAs(value);
    }

    @Test
    void dateTimeScalarRoundTrips() {
        Instant instant = Instant.parse("2026-10-16T12:00:00Z");
        Object serialized = DateTimeScalar.INSTANCE.getCoercing().serialize(instant, GraphQLContext.getDefault(), Locale.ROOT);
        assertThat(serialized).isEqualTo("2026-10-16T12:00:00Z");

        Object parsed = DateTimeScalar.INSTANCE.getCoercing()
                .parseValue("2026-10-16T07:00:00-05:00", GraphQLContext.getDefault(), Locale.ROOT);
        assertThat(parsed).isEqualTo(instant);

        Object literal = DateTimeScalar.INSTANCE.getCoercing().parseLiteral(
                StringValue.newStringValue("2026-10-16T12:00:00Z").build(), CoercedVariables.emptyVariables(),
                GraphQLContext.getDefault(), Locale.ROOT);
        assertThat(literal).isEqualTo(instant);
    }

    @Test
    void dateTimeScalarRejectsGarbage() {
        assertThatThrownBy(() -> DateTimeScalar.INSTANCE.getCoercing()
                .parseValue("yesterday", GraphQLContext.getDefault(), Locale.ROOT))
                .isInstanceOf(CoercingParseValueException.class);
    }
}
