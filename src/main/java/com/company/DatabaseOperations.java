package com.company;

import com.company.operations.MutatePath;
import com.company.operations.ReadPath;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@RequiredArgsConstructor
@Service
public class DatabaseOperations {

    private final ReadPath read;

    private final MutatePath mutate;

    public void put(String key, String value) {
        mutate.put(key, value);
    }

    public void update(String key, String value) {
        mutate.put(key, value);
    }

    public void remove(String key) {
        mutate.remove(key);
    }

    public String get(String key) {
        return read.get(key);
    }
}
