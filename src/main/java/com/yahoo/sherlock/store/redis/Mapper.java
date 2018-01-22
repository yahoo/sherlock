package com.yahoo.sherlock.store.redis;

import com.yahoo.sherlock.exception.StoreException;
import com.yahoo.sherlock.store.Attribute;
import com.yahoo.sherlock.utils.NumberUtils;
import com.yahoo.sherlock.utils.Utils;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * An abstract mapper implementation which transforms
 * a POJO into a map of key to values of each field.
 *
 * @param <T> the type in which to store the keys and values
 */
@Slf4j
public abstract class Mapper<T> {

    /**
     * Lazily initialized object class.
     * This value is set once per instance and it
     * is assumed that all hashed objects will be
     * of this type.
     */
    private Class<?> objClass = null;
    /**
     * Lazily initialized array of fields
     * that are hashed and unhashed.
     */
    private Field[] fields = null;

    /**
     * Initialize the hash mapper.
     *
     * @param cls the object class to hash
     */
    private void init(Class<?> cls) {
        objClass = cls;
        fields = Utils.findFields(objClass, Attribute.class);
        for (Field field : fields) {
            field.setAccessible(true);
        }
    }

    /**
     * On first run, the mapper will initialize the object
     * class and object fields, so that it does not need to
     * retrieve these values at each run.
     *
     * @param obj the object for which to check readiness
     * @throws StoreException if the given object's class does not match
     *                        the previous object class
     */
    private void checkReady(Object obj) {
        if (objClass == null) {
            init(obj.getClass());
        } else if (!objClass.equals(obj.getClass())) {
            throw new StoreException(String.format(
                    "Attempting to hash unknown class %s for %s",
                    obj.getClass().getCanonicalName(),
                    objClass.getCanonicalName()
            ));
        }
    }

    /**
     * On first run, the mapper will initialize the object
     * class and object fields, so that it does not need to
     * retrieve these values at each run.
     *
     * @param cls the class for which to check readiness
     * @throws StoreException if the given class does not match
     *                        the previous object class
     */
    private void checkReady(Class<?> cls) {
        if (objClass == null) {
            init(cls);
        } else if (!objClass.equals(cls)) {
            throw new StoreException(String.format(
                    "Attempting to unhash unknown class %s for %s",
                    cls.getCanonicalName(),
                    objClass.getCanonicalName()
            ));
        }
    }

    /**
     * This method should be used by subclasses to
     * retrieve the field array.
     *
     * @return the object serialized fields
     */
    protected Field[] getFields() {
        return fields;
    }

    /**
     * Create a new instance of the object that is mapped and unmapped
     * by this class. This method is used during unmapping.
     *
     * @param cls the class for which to construct an empty object
     * @param <C> the object type
     *            default constructor with no parameters
     * @return a new instance of the object
     */
    @NonNull
    protected <C> C construct(Class<C> cls) {
        Constructor<C> ctor;
        C obj;
        try {
            ctor = cls.getConstructor();
            obj = ctor.newInstance();
        } catch (NoSuchMethodException e) {
            log.error("Could not find default constructor: {}", cls);
            throw new StoreException(String.format(
                    "Class %s does not provide a default constructor",
                    cls.getSimpleName()
            ));
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            log.error("Error while instantiating object: {}", cls, e);
            throw new StoreException(String.format(
                    "Failed to instantiate new object of type %s",
                    cls.getSimpleName()
            ));
        }
        return obj;
    }

    /**
     * This method receives an object and maps its parameters
     * marked with {@code Attribute} to a map from the attribute
     * name to the attrifbute value.
     *
     * @param obj the object to serialized
     * @return a map from attribute name to value
     */
    public Map<T, T> map(Object obj) {
        checkReady(obj);
        return performMap(obj);
    }

    /**
     * This method receives a map of attribute names to attribute values
     * and deserializes them according to the provided class.
     *
     * @param cls the class to deserialize
     * @param map the attributes names and values
     * @param <C> the object type
     * @return a new object instance with the respective fields set
     */
    public <C> C unmap(Class<C> cls, Map<T, T> map) {
        checkReady(cls);
        return performUnmap(cls, map);
    }

    /**
     * This method is implemented by subclasses and performs the actual
     * deserialization of the object.
     *
     * @param obj object to deserialize
     * @return a map of attribute names to values
     */
    abstract protected Map<T, T> performMap(Object obj);

    /**
     * This method is implemented by subclasses and performs the deserialization
     * of attribute maps to objects.
     *
     * @param cls the class of the object to deserialize
     * @param map the attribute key-value map
     * @param <C> the object type
     * @return a new object instance
     */
    abstract protected <C> C performUnmap(Class<C> cls, Map<T, T> map);

    /**
     * @param str String to encode as bytes
     * @return String bytes using UTF-8
     */
    public static byte[] encode(String str) {
        if (str == null) {
            return null;
        }
        return str.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Container class for an arbitrary type.
     *
     * @param <T> contained type
     */
    public static abstract class Box<T> {
        private final T t;

        /**
         * @param t type instance to contain
         */
        private Box(T t) {
            this.t = t;
        }

        /**
         * @return contained type instance
         */
        public T get() {
            return t;
        }
    }

    /**
     * Container for an integer.
     */
    private static class IntBox extends Box<Integer> {
        /**
         * @param integer value to contain
         */
        private IntBox(Integer integer) {
            super(integer);
        }
    }

    /**
     * Container for a long.
     */
    private static class LongBox extends Box<Long> {
        /**
         * @param aLong value to contain
         */
        private LongBox(Long aLong) {
            super(aLong);
        }
    }

    /**
     * Container for a double.
     */
    private static class DoubleBox extends Box<Double> {
        /**
         * @param aDouble value to contain
         */
        private DoubleBox(Double aDouble) {
            super(aDouble);
        }
    }

    /**
     * Container for a String.
     */
    private static class StringBox extends Box<String> {
        /**
         * @param string value to contain
         */
        private StringBox(String string) {
            super(string);
        }
    }

    private static Class<?>[] CLASS_ARRAY = {
        Integer.class,
        Long.class,
        Double.class,
        String.class
    };
    private static Attribute.Type[] TYPES = {
        Attribute.Type.INTEGER,
        Attribute.Type.LONG,
        Attribute.Type.DOUBLE,
        Attribute.Type.STRING
    };

    /**
     * Get the attribute type for a field. Attempts to resolve
     * using the field class if a field is not specified.
     *
     * @param field field whose type to resolve
     * @return the type for the field
     */
    public static Attribute.Type resolveType(Field field) {
        Attribute.Type type = field.getAnnotation(Attribute.class).type();
        if (type != Attribute.Type.UNSPECIFIED) {
            return type;
        }
        for (int i = 0; i < CLASS_ARRAY.length - 1; i++) {
            if (CLASS_ARRAY[i].equals(field.getType())) {
                return TYPES[i];
            }
        }
        return Attribute.Type.STRING;
    }

    /**
     * Wrap the value of a field as the appropriate type.
     *
     * @param field  field whose value to wrap
     * @param strVal the string value of the field
     * @param <T>    the value type
     * @return a container for the value
     */
    @SuppressWarnings("unchecked")
    public static <T> Box<T> wrapType(Field field, String strVal) {
        Attribute.Type type = resolveType(field);
        switch (type) {
            case INTEGER:
                return (Box<T>) new IntBox(NumberUtils.parseInt(strVal));
            case LONG:
                return (Box<T>) new LongBox(NumberUtils.parseLong(strVal));
            case DOUBLE:
                return (Box<T>) new DoubleBox(NumberUtils.parseDouble(strVal));
            default:
                return (Box<T>) new StringBox(strVal);
        }
    }

}
