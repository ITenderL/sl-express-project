package com.sl.transport.service.impl;

import com.sl.transport.entity.node.TLTEntity;
import com.sl.transport.repository.TLTRepository;
import com.sl.transport.service.TLTService;
import org.springframework.stereotype.Service;

@Service(value = "tltService")
public class TLTServiceImpl extends ServiceImpl<TLTRepository, TLTEntity>
        implements TLTService {

}
