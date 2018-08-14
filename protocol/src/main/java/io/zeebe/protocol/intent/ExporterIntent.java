package io.zeebe.protocol.intent;

public enum ExporterIntent implements Intent {
  POSITIONS_COMMITTED((short) 0);

  private short value;

  ExporterIntent(short value) {
    this.value = value;
  }

  public short getIntent() {
    return value;
  }

  public static Intent from(short value) {
    switch (value) {
      case 0:
        return POSITIONS_COMMITTED;
      default:
        return Intent.UNKNOWN;
    }
  }

  @Override
  public short value() {
    return value;
  }
}
