package com.sl.ms.search.service.impl;

import com.sl.ms.search.domain.dto.CourierTaskDTO;
import com.sl.ms.search.domain.dto.CourierTaskPageQueryDTO;
import com.sl.ms.search.entity.CourierTaskEntity;
import com.sl.ms.search.service.CourierTaskService;
import com.sl.transport.common.util.PageResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author analytics
 * @date 2024/11/5 10:08
 * @description
 */
@Slf4j
@Service
public class CourierTaskServiceImpl implements CourierTaskService {

    @Override
    public PageResponse<CourierTaskDTO> pageQuery(CourierTaskPageQueryDTO pageQueryDTO) {
        return null;
    }

    @Override
    public void saveOrUpdate(CourierTaskDTO courierTaskDTO) {

    }

    @Override
    public CourierTaskDTO findById(Long id) {
        return null;
    }

    @Override
    public List<CourierTaskEntity> findByOrderId(Long orderId) {
        return null;
    }
}
