package com.zakgof.eb;

import com.zakgof.velvet.IVelvetEnvironment;
import com.zakgof.velvet.VelvetFactory;
import com.zakgof.velvet.entity.Entities;
import com.zakgof.velvet.entity.IEntityDef;
import one.microstream.collections.lazy.LazyHashMap;
import one.microstream.storage.embedded.types.EmbeddedStorage;
import one.microstream.storage.embedded.types.EmbeddedStorageManager;
import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Main {

    private static final Path rootPath = Paths.get(System.getProperty("user.home"), ".zakgof", "embedded-benchmark");
    private static final Path velvetPath = rootPath.resolve("velvet");
    private static final Path msEagerMapPath = rootPath.resolve("microstream-eager-map");
    private static final Path msLazyMapPath = rootPath.resolve("microstream-lazy-map");

    private static final int RECORDS = 3_000_000;

    private static final Random random = new Random(1L);
    private static final IEntityDef<String, User> userEntity = Entities.from(User.class)
            .make(String.class, User::getUsername);

    private Map<String, User> users;
    private String pickedKey;
    private String pickedLastName;

    public static void main(String[] args) {
        new Main().run();
    }

    Main() {
        createData();
        createVelvetDb();
        createMicroStreamEagerMap();
        createMicroStreamLazyMap();
    }

    private void createData() {
        users = generateUsers(RECORDS);
        User pickedUser = users.values().iterator().next();
        pickedKey = pickedUser.getUsername();
        pickedLastName = pickedUser.getLastName();
    }

    private void createVelvetDb() {
        cleanDir(velvetPath);
        try (IVelvetEnvironment velvetEnvironment = openVelvet()) {
            userEntity.put().values(users.values()).execute(velvetEnvironment);
        }
    }

    private void createMicroStreamEagerMap() {
        cleanDir(msEagerMapPath);
        try (EmbeddedStorageManager storageManager = EmbeddedStorage.start(msEagerMapPath)) {
            storageManager.setRoot(users);
            storageManager.storeRoot();
        }
    }

    private void createMicroStreamLazyMap() {
        cleanDir(msLazyMapPath);
        Map<String, User> lazyUsers = new LazyHashMap<>();
        lazyUsers.putAll(users);
        try (EmbeddedStorageManager storageManager = EmbeddedStorage.start(msLazyMapPath)) {
            storageManager.setRoot(lazyUsers);
            storageManager.storeRoot();
        }
    }

    private Map<String, User> generateUsers(int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> generateUser())
                .collect(Collectors.toMap(User::getUsername, Function.identity()));
    }

    private IVelvetEnvironment openVelvet() {
        return VelvetFactory.create("xodus", velvetPath.toUri().toString());
    }

    private void cleanDir(Path path) {
        try {
            FileUtils.deleteDirectory(path.toFile());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static User generateUser() {
        return new User(
                generateString(16),
                generateString(32),
                generateString(32),
                LocalDate.of(1900 + random.nextInt(100), 1 + random.nextInt(12), 1 + random.nextInt(28))
        );
    }

    private static String generateString(int len) {
        return IntStream.range(0, len)
                .map(i -> (int) 'a' + random.nextInt(26))
                .mapToObj(c -> "" + (char) c)
                .collect(Collectors.joining());
    }

    private void run() {
        measure("velvet", this::readVelvetDb);
        measure("ms eager", this::readMicroStreamEagerMap);
        measure("ms lazy", this::readMicroStreamLazyMap);
    }

    @SuppressWarnings("unchecked")
    private User readMicroStreamEagerMap() {
        try (EmbeddedStorageManager storageManager = EmbeddedStorage.start(msEagerMapPath)) {
            Map<String, User> users = (Map<String, User>) storageManager.root();
            return users.get(pickedKey);
        }
    }

    @SuppressWarnings("unchecked")
    private User readMicroStreamLazyMap() {
        try (EmbeddedStorageManager storageManager = EmbeddedStorage.start(msLazyMapPath)) {
            Map<String, User> users = (Map<String, User>) storageManager.root();
            return users.get(pickedKey);
        }
    }

    private void measure(String label, Supplier<User> procedure) {
        long start = System.currentTimeMillis();
        User user = procedure.get();
        long end = System.currentTimeMillis();
        System.out.println(label + " " + (end - start) + " ms");
        if (!user.getLastName().equals(pickedLastName)) {
            throw new RuntimeException("Data mismatch");
        }
    }

    private User readVelvetDb() {
        try (IVelvetEnvironment velvetEnvironment = openVelvet()) {
            return userEntity.get().key(pickedKey)
                    .execute(velvetEnvironment);
        }
    }
}
