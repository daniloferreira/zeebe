/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.exporter;

import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.msgpack.property.LongProperty;
import io.zeebe.msgpack.property.StringProperty;
import org.agrona.DirectBuffer;

public class ExporterRecord extends UnpackedObject {
  public static final long POSITION_UNKNOWN = -1L;
  public static final String ID_UNKNOWN = "";

  private StringProperty idProperty = new StringProperty("id", ID_UNKNOWN);
  private LongProperty positionProperty = new LongProperty("position", POSITION_UNKNOWN);

  public ExporterRecord() {
    this.declareProperty(idProperty);
    this.declareProperty(positionProperty);
  }

  public long getPosition() {
    return positionProperty.getValue();
  }

  public DirectBuffer getId() {
    return idProperty.getValue();
  }

  public ExporterRecord setId(final String id) {
    idProperty.setValue(id);
    return this;
  }

  public ExporterRecord setPosition(final long position) {
    positionProperty.setValue(position);
    return this;
  }
}