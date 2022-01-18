package com.github.zjjfly.ce;

import java.util.Map;

import lombok.Data;

/**
 * @author Zi JunJie(junjie.zi@siemens.com)
 */
@Data
public class Parameter {

    Map<String,Object> paramMap;

    CombinationType combinationType;

}
