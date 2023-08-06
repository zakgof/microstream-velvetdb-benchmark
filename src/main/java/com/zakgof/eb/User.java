package com.zakgof.eb;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.time.LocalDate;

@RequiredArgsConstructor
@Getter
public class User {
    private final String username;
    private final String firstName;
    private final String lastName;
    private final LocalDate birthDate;
}
