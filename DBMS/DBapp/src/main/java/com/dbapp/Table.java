package com.dbapp;

import lombok.*;

import java.io.Serializable;
import java.util.List;

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Table implements Serializable {
    private static final long serialVersionUID = 1L;
    private String[] columnsNames;
    private List<String> traces;
}
