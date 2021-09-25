package io.kryptoworx.signalcli.storage;

public class Enums {

	public static <E extends Enum<E>> E fromOrdinal(Class<E> enumClass, int ordinal) {
		E[] values = enumClass.getEnumConstants();
		if (ordinal < 0 || ordinal >= values.length) {
			throw new IllegalArgumentException();
		}
		return values[ordinal];
	}

}
