package com.wzl.bigtable.hos.server;

import com.wzl.bigtable.common.BucketModel;
import com.wzl.bigtable.hos.core.authmgr.IAuthService;
import com.wzl.bigtable.hos.core.authmgr.model.ServiceAuth;
import com.wzl.bigtable.hos.core.usermgr.IUserService;
import com.wzl.bigtable.hos.core.usermgr.model.SystemRole;
import com.wzl.bigtable.hos.core.usermgr.model.UserInfo;
import com.wzl.bigtable.hos.mybatis.test.BaseTest;
import com.wzl.bigtable.hos.server.dao.BucketMapper;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

public class BucketMapperTest extends BaseTest {
    @Autowired
    BucketMapper bucketMapper;
    @Autowired
    @Qualifier("authServiceImpl")
    IAuthService authService;
    @Autowired
    @Qualifier("userServiceImpl")
    IUserService userService;

    @Test
    public void addBucket() {
        BucketModel bucketModel = new BucketModel("test1", "jixin", "");
        bucketMapper.addBucket(bucketModel);
        UserInfo userInfo = new UserInfo("jixin", "123456", SystemRole.ADMIN, "");
        userService.addUser(userInfo);
        ServiceAuth serviceAuth = new ServiceAuth();
        serviceAuth.setTargetToken(userInfo.getUserId());
        serviceAuth.setBucketName(bucketModel.getBucketName());
        authService.addAuth(serviceAuth);
        BucketModel bucketModel2 = new BucketModel("test2", "jixin", "");
        bucketMapper.addBucket(bucketModel2);
    }
}
