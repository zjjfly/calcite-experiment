package com.github.zjjfly.ce.converter;

import com.google.common.collect.HashMultimap;
import lombok.Data;

/**
 * @author Zi JunJie(junjie.zi@siemens.com)
 */
@Data
public class Parameter {

  HashMultimap<String, Object> kv;

  CombinationType combinationType;

}
