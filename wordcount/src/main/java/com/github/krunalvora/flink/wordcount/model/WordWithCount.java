package com.github.krunalvora.flink.wordcount.model;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString(includeFieldNames = false)
public class WordWithCount implements Serializable {
  private String word;
  private int count;
  private Dummy dummy;
}
