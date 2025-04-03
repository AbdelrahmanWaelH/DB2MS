package com.dbapp;

import lombok.*;

import java.io.Serializable;
import java.util.List;

@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Page implements Serializable {
	private static final long serialVersionUID = 1L;
    private List<String []> rows;

    public boolean isFull(int maxRows){
        return rows.size() == maxRows;
    }
}
