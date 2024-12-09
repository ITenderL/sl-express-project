package com.sl.ms.carriage.service;

import com.sl.ms.carriage.domain.dto.CarriageDTO;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.List;

/**
 * @author analytics
 * @date 2024/10/20 14:37
 * @description
 */
@SpringBootTest
class CarriageTest {

    @Resource
    private CarriageService carriageService;

    @Test
    void testFindAll() {
        List<CarriageDTO> all = carriageService.findAll();
        all.stream().limit(10).forEach(System.out::println);
    }
}
