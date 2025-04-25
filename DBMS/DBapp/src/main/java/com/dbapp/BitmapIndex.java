package com.dbapp;

import lombok.*;

import java.io.Serializable;
import java.util.Map;

@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class BitmapIndex implements Serializable {
    // stores the index as a map of the value and the index in binary
    // <a> -> <01110001001000111011011011>
    // <b> -> <10001110110111000100100100>
    private Map<String,String> index;
    private String columnName;
    private String tableName;
    private int columnIndex;
}
