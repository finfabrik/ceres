package com.blokaly.ceres.binding;

import com.blokaly.ceres.health.HealthChecker;
import com.codahale.metrics.health.HealthCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@CeresService
public abstract class AwaitExecutionService extends ExecutionService implements HealthChecker {
  protected final Logger LOGGER = LoggerFactory.getLogger(getClass());

  private final HealthCheck checker = new HealthCheck() {
    @Override
    protected Result check() throws Exception {
      return AwaitExecutionService.this.diagnosis();
    }
  };

  @Override
  public HealthCheck getChecker() {
    return checker;
  }

  @Override
  public HealthCheck.Result diagnosis() {
    return HealthCheck.Result.healthy();
  }

  @Override
  protected void run() throws Exception {
    awaitTerminated();
  }
}
