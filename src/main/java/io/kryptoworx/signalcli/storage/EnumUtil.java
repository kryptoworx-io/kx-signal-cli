package io.kryptoworx.signalcli.storage;

import java.util.Collection;
import java.util.EnumSet;

public class EnumUtil {
    public static <T extends Enum<T>> int toInt(Collection<T> values) {
        if (values == null) return 0;
        int r = 0;
        for (T v : values) {
            int ordinal = v.ordinal();
            if (ordinal > 31) throw new IllegalArgumentException();
            r |= (1 << v.ordinal());
        }
        return r;
    }

    public static <T extends Enum<T>> EnumSet<T> fromInt(Class<T> enumClass, int set) {
        T[] values = enumClass.getEnumConstants();
        EnumSet<T> result = EnumSet.noneOf(enumClass);
        for (int i = 0; set != 0; set >>>= 1, i++) {
            if ((set & 1) == 1) {
                result.add(values[i]);
            }
        }
        return result;
    }

    public static <T extends Enum<T>> T fromOrdinal(Class<T> enumClass, int ordinal) {
        T[] values = enumClass.getEnumConstants();
        if (ordinal >= values.length) {
            throw new IllegalArgumentException();
        }
        return values[ordinal];
    }
}
