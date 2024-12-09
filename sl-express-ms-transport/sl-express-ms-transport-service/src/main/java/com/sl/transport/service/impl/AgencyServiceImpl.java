package com.sl.transport.service.impl;

import com.sl.transport.entity.node.AgencyEntity;
import com.sl.transport.repository.AgencyRepository;
import com.sl.transport.service.AgencyService;
import org.springframework.stereotype.Service;

/**
 * @author zzj
 * @version 1.0
 */
@Service(value = "agencyService")
public class AgencyServiceImpl extends ServiceImpl<AgencyRepository, AgencyEntity> implements AgencyService {

}
