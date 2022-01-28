package com.company;

import com.company.service.UpdatePath;
import com.company.service.ReadPath;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@RequiredArgsConstructor
@Service
public class DatabaseNode {

    private final ReadPath read;

    private final UpdatePath update;

    public void put(String key, String value) {
        update.put(key, value);
    }

    public void update(String key, String value) {
        update.put(key, value);
    }

    public void remove(String key) {
        update.remove(key);
    }

    public String get(String key) {
        return read.get(key);
    }
}
