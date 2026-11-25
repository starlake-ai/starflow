package ai.starlake.dbt.jinja;

import com.hubspot.jinjava.interpret.JinjavaInterpreter;
import com.hubspot.jinjava.lib.exptest.IsMappingExpTest;

/**
 * {@code is mapping}, true for a relation too, as dbt's Relation is a mapping
 * (dbt_utils._is_relation tests it). In Java: Jinjava calls {@code evaluate} as a varargs method.
 */
public final class DbtIsMapping extends IsMappingExpTest {
  @Override
  public boolean evaluate(Object var, JinjavaInterpreter interpreter, Object... args) {
    return var instanceof DbtJinjaRelation || super.evaluate(var, interpreter, args);
  }
}
